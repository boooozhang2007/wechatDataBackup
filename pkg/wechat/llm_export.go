package wechat

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"
)

type LLMMessage struct {
	Content   string `json:"Content"`
	Sender    string `json:"Sender"`
	TimeStr   string `json:"TimeStr"`
	Timestamp int64  `json:"Timestamp"`
	IsSender  int    `json:"IsSender"`
}

type LLMSessionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Pre-compiled regex patterns (Go doesn't support lookbehind, use simpler patterns)
var (
	rePhone = regexp.MustCompile(`1[3-9]\d{9}`)
	reEmail = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	reURL   = regexp.MustCompile(`https?://[^\s<>"{}|\\^\[\]` + "`" + `]+`)
	reID    = regexp.MustCompile(`[1-9]\d{5}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]`)
)

func CleanPII(text string) string {
	if text == "" {
		return text
	}
	
	// Phone numbers (Chinese mobile)
	text = rePhone.ReplaceAllString(text, "[PHONE_REMOVED]")

	// ID cards (Chinese ID)
	text = reID.ReplaceAllString(text, "[ID_REMOVED]")

	// Emails
	text = reEmail.ReplaceAllString(text, "[EMAIL_REMOVED]")

	// URLs
	text = reURL.ReplaceAllString(text, "[URL_REMOVED]")

	return text
}

func (P *WechatDataProvider) ExportRawMessages(wxid string) ([]LLMMessage, error) {
	var allMessages []LLMMessage

	if P == nil {
		return allMessages, fmt.Errorf("provider is nil")
	}

	// Get contacts for name resolution
	contactsMap := make(map[string]string)
	contactsList, err := P.wechatGetAllContact()
	if err == nil && contactsList != nil {
		for _, contact := range contactsList.Users {
			name := contact.ReMark
			if name == "" {
				name = contact.NickName
			}
			if name == "" {
				name = contact.UserName
			}
			contactsMap[contact.UserName] = name
		}
	}

	for _, msgDB := range P.msgDBs {
		if msgDB == nil || msgDB.db == nil {
			continue
		}
		
		// Escape wxid to prevent SQL injection
		query := "SELECT StrTalker, StrContent, CreateTime, IsSender FROM MSG WHERE Type = 1"
		if wxid != "" {
			safeWxid := strings.ReplaceAll(wxid, "'", "''")
			query += fmt.Sprintf(" AND StrTalker = '%s'", safeWxid)
		}

		rows, err := msgDB.db.Query(query)
		if err != nil {
			log.Printf("LLM Export query error: %v", err)
			continue // Skip if error
		}
		// Don't use defer inside a loop, it will stack up and only close at function exit
		// defer rows.Close()

		for rows.Next() {
			var talkerID string
			var content sql.NullString
			var createTime int64
			var isSender int
			if err := rows.Scan(&talkerID, &content, &createTime, &isSender); err != nil {
				continue
			}

			if !content.Valid {
				continue
			}

			senderName := talkerID
			if isSender == 1 {
				senderName = "我"
			} else {
				if name, ok := contactsMap[talkerID]; ok {
					senderName = name
				}
			}

			allMessages = append(allMessages, LLMMessage{
				Content:   content.String,
				Sender:    senderName,
				TimeStr:   time.Unix(createTime, 0).Format("2006-01-02 15:04:05"),
				Timestamp: createTime,
				IsSender:  isSender,
			})
		}
		rows.Close()
	}

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp < allMessages[j].Timestamp
	})

	return allMessages, nil
}

func ConvertToJSONL(rawMessages []LLMMessage, systemPrompt, botRoleName string, splitGapMin int, cleanPii bool) ([]interface{}, error) {
	var sessions []interface{}
	var currentSession []LLMSessionMessage
	var lastTime int64

	for _, msg := range rawMessages {
		if lastTime > 0 && (msg.Timestamp-lastTime) > int64(splitGapMin*60) {
			if len(currentSession) > 0 {
				sessions = append(sessions, currentSession)
			}
			currentSession = []LLMSessionMessage{}
		}

		isMe := (msg.Sender == "我" || msg.IsSender == 1)
		role := "user"

		if (botRoleName == "我(训练自己)" && isMe) || (botRoleName != "我(训练自己)" && !isMe) {
			role = "assistant"
		}

		content := msg.Content
		if cleanPii {
			content = CleanPII(content)
		}

		currentSession = append(currentSession, LLMSessionMessage{
			Role:    role,
			Content: content,
		})
		lastTime = msg.Timestamp
	}

	if len(currentSession) > 0 {
		sessions = append(sessions, currentSession)
	}

	return sessions, nil
}
