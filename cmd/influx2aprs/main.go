package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/acobaugh/aprs"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var (
	fConfig      string
	fDebug       bool
	fOnce        bool
	fPrintConfig bool

	defaultConfig = []byte(`
callsign: ""
ssid: 13
interval: 10m
lat: ""
lon: ""
comment: github.com/acobaugh/aprs-tools
influxdb:
  url: http://localhost:8086
  db: rtl_433_wx
  measurement: Fineoffset-WH24
  rp: autogen
  station: 10
`)
)

func init() {
	flag.StringVarP(&fConfig, "config", "c", "", "config file")
	flag.BoolVarP(&fDebug, "debug", "d", false, "enable debug output")
	flag.BoolVarP(&fOnce, "once", "o", false, "run once then exit")
	flag.BoolVarP(&fPrintConfig, "print-config", "P", false, "print default config then exit")
	flag.Parse()
}

func main() {
	log := logrus.StandardLogger()

	viper.SetConfigType("yaml")

	// read default config
	err := viper.ReadConfig(bytes.NewBuffer(defaultConfig))
	if err != nil {
		log.WithError(err).Fatal("failed to parse default config")
	}

	// read config file from given path
	if fConfig != "" {
		viper.SetConfigFile(fConfig)
		err := viper.MergeInConfig()
		if err != nil {
			log.WithError(err).Fatal("fatal error config file")
		}
	}

	// allow env vars to override config
	viper.AutomaticEnv()

	// print parsed config
	if fDebug {
		c := viper.AllSettings()
		b, err := yaml.Marshal(c)
		if err != nil {
			log.WithError(err).Fatal("failed to marshal default config to yaml")
		}
		fmt.Print(string(b))
	}

	if fDebug {
		log.SetLevel(logrus.DebugLevel)
		log.SetFormatter(&logrus.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	interval, err := time.ParseDuration(viper.GetString("interval"))
	if err != nil {
		log.WithError(err).Fatal("Failed to parse interval")
	}

	influx := influxdb2.NewClient(viper.GetString("influxdb.url"), "")
	queryAPI := influx.QueryAPI("")

	var lastTime time.Time
	ticker := time.NewTicker(interval)
LOOP:
	for ; true; <-ticker.C {
		var wxData aprs.Wx
		wxData.Zero()
		wxData.Lat = viper.GetFloat64("lat")
		wxData.Lon = viper.GetFloat64("lon")
		wxData.Type = viper.GetString("comment")

		result, err := queryAPI.Query(
			context.TODO(), fmt.Sprintf(
				`from(bucket: "%s/%s")
				|> range(start: -%s)
				|> filter(fn: (r) => r._measurement == "%s" and r.id == "%s")
				|> limit(n:1)`,
				viper.GetString("influxdb.db"),
				viper.GetString("influxdb.rp"),
				interval*2,
				viper.GetString("influxdb.measurement"),
				viper.GetString("influxdb.station"),
			),
		)

		if err == nil {
			for result.Next() {
				if wxData.Timestamp.IsZero() {
					wxData.Timestamp = result.Record().Time()
					if wxData.Timestamp == lastTime || wxData.Timestamp.IsZero() {
						log.Debugf("skipping. timestamp=%s lastTime=%s", wxData.Timestamp, lastTime)
						continue LOOP
					}
					lastTime = wxData.Timestamp
				}

				switch result.Record().Field() {
				case "temperature_C":
					wxData.Temp = int(math.Round(result.Record().Value().(float64)*1.8 + 32))
				case "humidity":
					wxData.Humidity = int(math.Round(result.Record().Value().(float64)))
				case "light_lux":
					wxData.SolarRad = int(math.Round(result.Record().Value().(float64)) / 126) // lux / 126 = W/m^2
				case "wind_dir_deg":
					wxData.WindDir = int(math.Round(result.Record().Value().(float64)))
				case "wind_max_m_s":
					wxData.WindGust = int(math.Round(result.Record().Value().(float64) * 2.23694))
				case "wind_avg_m_s":
					wxData.WindSpeed = int(math.Round(result.Record().Value().(float64) * 2.23694))
				}
			}

			if result.Err() != nil {
				log.WithError(result.Err()).Error("Result error")
			}
		} else {
			log.WithError(err).Error("Query error")
		}
		if !wxData.Timestamp.IsZero() {
			log.Debugf("wxData: %#v", wxData)

			f := aprs.Frame{
				Dst:  aprs.Addr{Call: "APRS"},
				Src:  aprs.Addr{Call: viper.GetString("callsign"), SSID: viper.GetInt("ssid")},
				Path: aprs.Path{aprs.Addr{Call: "TCPIP", Repeated: true}},
				Text: wxData.String(),
			}
			err := f.SendIS("tcp://rotate.aprs.net:14580", int(aprs.GenPass(f.Src.Call)))
			if err != nil {
				log.WithError(err).Error("APRS-IS error")
				continue
			}
			log.Infof("Sent to APRS-IS: %s", f)

			if fOnce {
				os.Exit(0)
			}
		} else {
			log.Debug("empty wxData")
		}
	}
}
