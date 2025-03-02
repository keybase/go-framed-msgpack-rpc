// Copyright 2018 Keybase, Inc. All rights reserved. Use of
// this source code is governed by the included BSD license.

package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

const ConnectionLogMsgKey string = "msg"

type LogField struct {
	Key   string
	Value interface{}
}

func (l LogField) String() string {
	return fmt.Sprintf("%s=%v", l.Key, l.Value)
}

// Join all the Key/Values together with equal signs. If there are >0
// of them, then add the prefix, otherwise, just return an empty string.
func LogFieldsToString(lfs []LogField, prfx string) string {
	if len(lfs) == 0 {
		return ""
	}
	var tmp []string
	for _, lf := range lfs {
		tmp = append(tmp, lf.String())
	}
	return prfx + strings.Join(tmp, " ")
}

// Format implements the fmt.Formatter interface, to make the structured
// LogField compatible with format-based non-structured loggers.
func (l LogField) Format(s fmt.State, verb rune) {
	fmt.Fprintf(s, "%"+string(verb), l.Value)
}

// ConnectionLog defines an interface used by connection.go for logging. An
// implementation that does structural logging may ignore `format` completely
// if `ConnectionLogMsgKey` is provided in LogField.
type ConnectionLog interface {
	Warnw(format string, fields ...LogField)
	Debugw(format string, fields ...LogField)
	Infow(format string, fields ...LogField)
}

type ConnectionLogFactory interface {
	Make(section string) ConnectionLog
}

type connectionLogUnstructured struct {
	LogOutput
	logPrefix string
}

func newConnectionLogUnstructured(
	logOutput LogOutputWithDepthAdder, prefix string) *connectionLogUnstructured {
	randBytes := make([]byte, 4)
	_, _ = rand.Read(randBytes)
	return &connectionLogUnstructured{
		LogOutput: logOutput.CloneWithAddedDepth(1),
		logPrefix: strings.Join(
			[]string{prefix, hex.EncodeToString(randBytes)}, " "),
	}
}

func formatLogFields(f string, lf ...LogField) string {
	lfs := LogFieldsToString(lf, " ")
	return f + lfs
}

func (l *connectionLogUnstructured) Warnw(
	format string, fields ...LogField) {
	l.LogOutput.Warnf("(%s) %s", l.logPrefix,
		formatLogFields(format, fields...))
}

func (l *connectionLogUnstructured) Debugw(
	format string, fields ...LogField) {
	l.LogOutput.Debugf("(%s) %s", l.logPrefix,
		formatLogFields(format, fields...))
}

func (l *connectionLogUnstructured) Infow(
	format string, fields ...LogField) {
	l.LogOutput.Infof("(%s) %s", l.logPrefix,
		formatLogFields(format, fields...))
}
