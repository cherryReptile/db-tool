package config

import (
	"github.com/spf13/viper"
)

func ReadConfig(configPath string) error {
	viper.AddConfigPath(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return nil
}
