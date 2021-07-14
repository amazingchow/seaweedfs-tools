package pkg

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
)

type CipherKey []byte

func Encrypt(plaintext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func Decrypt(ciphertext []byte, key CipherKey) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func GetCipherKey() ([]byte, error) {
	// ENCRYPTION_KEY是一个双重base64加密的字符串
	doubleEncCK := os.Getenv("ENCRYPTION_KEY")
	if len(doubleEncCK) == 0 {
		return nil, errors.New("ENCRYPTION_KEY env is empty")
	}
	encCK, err := base64.StdEncoding.DecodeString(doubleEncCK)
	if err != nil {
		return nil, fmt.Errorf("failed to decode DoubleEncCK, err: %v", err)
	}
	ck, err := base64.StdEncoding.DecodeString(string(encCK))
	if err != nil {
		return nil, fmt.Errorf("failed to decode EncCK, err: %v", err)
	}
	if len(ck) != 16 {
		return nil, fmt.Errorf("ENCRYPTION_KEY is invalid, got %d-len key", len(ck))
	}
	return ck, nil
}
