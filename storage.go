package kv

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type Storage struct {
	path 	 string
	mutex 	 sync.Mutex
	file 	 *os.File
	_storage map[string]interface{}
}

type Config struct {
	FilePath string
}

func Open(config *Config) (*Storage, error) {
	if config == nil {
		return nil, errors.New("config is empty")
	}

	if config.FilePath == "" {
		return nil, errors.New("config path is empty")
	}

	storage := &Storage{
		path: config.FilePath,
		_storage: make(map[string]interface{}),
	}

	e := make(chan error)
	defer close(e)
	go func() {
		e <- storage.load()
	}()

	if err := <-e; err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *Storage) load() (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.file, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return errors.Wrap(err, "open from load")
	}

	scanner := bufio.NewScanner(s.file)
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ":")
		if len(line) < 2 {
			return errors.New("unexpected storage key-value pair")
		}

		src, err := hex.DecodeString(line[0])
		if err != nil {
			return errors.Wrap(err, "decoding key")
		}

		key, err := decrypt(src, []byte(encryptKey))
		if err != nil {
			return errors.Wrap(err, "decrypting key")
		}

		var ev encodeValue
		err = gob.NewDecoder(bytes.NewBuffer([]byte(line[1]))).Decode(&ev)
		if err != nil {
			return errors.Wrap(err, "bytes gob decode value")
		}

		s._storage[string(key)] = ev.Value
	}

	return nil
}

type encodeValue struct {
	Value interface{}
}

func (s *Storage) Set(key string, value interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()


	keyEncrypt, err := encrypt([]byte(key), []byte(encryptKey))
	if err != nil {
		return errors.Wrap(err, "encrypt key")
	}

	valueBuff := bytes.NewBuffer([]byte{})
	err = gob.NewEncoder(valueBuff).Encode(encodeValue{
		Value: value,
	})
	if err != nil {
		return errors.Wrap(err, "encrypting value")
	}

	_, err = s.file.WriteString(fmt.Sprintf("%x:%s\n", keyEncrypt, valueBuff.String()))
	if err != nil {
		return errors.Wrap(err, "set key-value pair to storage")
	}

	s._storage[key] = value

	return nil
}

func (s *Storage) Get(key string) (interface{}, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	v, ok := s._storage[key]
	if !ok {
		return nil, errors.Errorf("not found value from key %s", key)
	}

	return v, nil
}

func (s *Storage) Close() error {
	return s.file.Close()
}

func encrypt(plaintext []byte, key []byte) ([]byte, error) {
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

func decrypt(ciphertext []byte, key []byte) ([]byte, error) {
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

const encryptKey = "ngjkwrnkjrngjkretnjkgetr"

