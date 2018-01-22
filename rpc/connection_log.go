// Copyright 2018 Keybase, Inc. All rights reserved. Use of
// this source code is governed by the included BSD license.

package rpc

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
)

type logField struct {
	key   string
	value interface{}
}

// Format implements the fmt.Formatter interface, to make the structured
// logField compatible with format-based non-structured loggers.
func (f logField) Format(s fmt.State, verb rune) {
	fmt.Fprintf(s, "%"+string(verb), f.value)
}

// connectionLog defines an interface used by connection.go for logging. An
// implementation typically only uses either of `msg` and `format` field,
// depending on if the logger is structured or not.
type connectionLog interface {
	Warning(msg string, format string, fields ...logField)
	Debug(msg string, format string, fields ...logField)
	Info(msg string, format string, fields ...logField)
}

type connectionLogUnstructured struct {
	LogOutput
	logPrefix string
}

func (l *connectionLogUnstructured) format(f string, lf ...logField) string {
	fields := make([]interface{}, 0, len(lf))
	for _, lf := range lf {
		fields = append(fields, lf)
	}
	return fmt.Sprintf(f, fields...)
}

func (l *connectionLogUnstructured) Warning(
	msg string, format string, fields ...logField) {
	l.LogOutput.Warning("(%s) %s", l.logPrefix, l.format(format, fields...))
}

func (l *connectionLogUnstructured) Debug(
	msg string, format string, fields ...logField) {
	l.LogOutput.Debug("(%s) %s", l.logPrefix, l.format(format, fields...))
}

func (l *connectionLogUnstructured) Info(
	msg string, format string, fields ...logField) {
	l.LogOutput.Info("(%s) %s", l.logPrefix, l.format(format, fields...))
}

// LogrusEntry and LogrusLogger define methods we need from logrus to avoid
// pulling logrus as dependency.
type LogrusEntry interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warning(args ...interface{})
}

// LogrusLogger maps to *logrus.Logger, but will need an adapter that convers
// map[string]interface{} to logrus.Fields, and adapt logrus.Entry to
// LogrusEntry.
type LogrusLogger interface {
	WithFields(map[string]interface{}) LogrusEntry
}

type connectionLogLogrus struct {
	log LogrusLogger

	section string
}

func newConnectionLogLogrus(
	log LogrusLogger, section string) *connectionLogLogrus {
	randBytes := make([]byte, 4)
	rand.Read(randBytes)
	return &connectionLogLogrus{
		log:     log,
		section: section + "-" + hex.EncodeToString(randBytes),
	}
}

func (l *connectionLogLogrus) logSkip(fields ...logField) LogrusEntry {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file, line = "unknown", -1
	}
	mFields := make(map[string]interface{})
	for _, f := range fields {
		mFields[f.key] = f.value
	}
	mFields["section"] = l.section
	mFields["file"], mFields["line"] = file, line
	return l.log.WithFields(mFields)
}

func (l *connectionLogLogrus) Warning(
	msg string, format string, fields ...logField) {
	l.logSkip(fields...).Warning(msg)
}

func (l *connectionLogLogrus) Debug(
	msg string, format string, fields ...logField) {
	// Nasty hack to lift Debug to Info: we can't just do Info everywhere in
	// this package since they'll leak to CLI in keybase/client. We also can't
	// just turn on Debug logging on server-side. Since logrus loggers are only
	// used by server side, we simply override Debug with Info here.
	l.logSkip(fields...).Info(msg)
}

func (l *connectionLogLogrus) Info(
	msg string, format string, fields ...logField) {
	l.logSkip(fields...).Info(msg)
}
