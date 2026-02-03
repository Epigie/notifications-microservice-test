package models

type Message struct {
	ID      uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	Content string `gorm:"type:text;not null" json:"content"`
	Status  string `gorm:"type:varchar(50);not null" json:"status"`
}
