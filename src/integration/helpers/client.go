package helpers

import (
	influxdb "github.com/influxdb/influxdb-go"
	. "launchpad.net/gocheck"
)

type Client interface {
	RunQuery(query string, c *C, timePrecision ...influxdb.TimePrecision) []*influxdb.Series
	RunInvalidQuery(query string, c *C, timePrecision ...influxdb.TimePrecision) []*influxdb.Series
	WriteData(series []*influxdb.Series, c *C, timePrecision ...influxdb.TimePrecision)
	WriteJsonData(series string, c *C, timePrecision ...influxdb.TimePrecision)
}
