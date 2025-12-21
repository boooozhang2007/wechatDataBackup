package wechat

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
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

func CleanPII(text string) string {
	// Phone numbers
	rePhone := regexp.MustCompile(`(?<!\d)1[3-9]\d{9}(?!\d)`)
	text = rePhone.ReplaceAllString(text, "[PHONE_REMOVED]")

	// ID cards
	reID := regexp.MustCompile(`(?<!\d)[1-9]\d{5}(18|19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)`)
	text = reID.ReplaceAllString(text, "[ID_REMOVED]")

	// Emails
	reEmail := regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	text = reEmail.ReplaceAllString(text, "[EMAIL_REMOVED]")

	// URLs
	reURL := regexp.MustCompile(`(http|https)://[a-zA-Z0-9./?=_-]+`)
	text = reURL.ReplaceAllString(text, "[URL_REMOVED]")

	return text
}

func (P *WechatDataProvider) ExportRawMessages(wxid string) ([]LLMMessage, error) {
	var allMessages []LLMMessage

	// Get contacts for name resolution
	contactsList, err := P.wechatGetAllContact()
	contactsMap := make(map[string]string)
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
		if msgDB.db == nil {
			continue
		}
		query := "SELECT StrTalker, StrContent, CreateTime, IsSender FROM MSG WHERE Type = 1"
		if wxid != "" {
			query += fmt.Sprintf(" AND StrTalker = '%s'", wxid)
		}

		rows, err := msgDB.db.Query(query)
		if err != nil {
			continue // Skip if error
		}
		defer rows.Close()

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
