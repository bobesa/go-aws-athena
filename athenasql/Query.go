package athenasql

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"
)

func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Name:    fmt.Sprintf("$%d", i+1),
			Ordinal: i,
			Value:   arg,
		}
	}
	return namedArgs
}

func queryWithArgs(query string, args []driver.NamedValue) (string, error) {
	hasAnonymousArgs := strings.Contains(query, "?")
	hasPositionalArgs := strings.Contains(query, "$")

	switch {
	case len(args) == 0 && !hasAnonymousArgs && !hasPositionalArgs:
		return query, nil

	case hasAnonymousArgs && hasPositionalArgs:
		return "", fmt.Errorf(`%q has both positional ($[0-9]+) and anonymous (?) arguments`, query)

	case hasAnonymousArgs:
		if argCount := strings.Count(query, "?"); argCount != len(args) {
			return "", fmt.Errorf(`%q expects %d arguments, but %d arguments were supplied`, query, argCount, len(args))
		}
		for _, arg := range args {
			query = strings.Replace(query, "?", fmt.Sprint(arg.Value), 1)
		}

	case hasPositionalArgs:
		var s scanner.Scanner
		s.Init(strings.NewReader(query))
		s.Filename = "<source>"
		s.IsIdentRune = func(ch rune, i int) bool {
			return strings.Contains(string(ch), "$") || unicode.IsLetter(ch) || unicode.IsDigit(ch)
		}

		argMap := make(map[int]string)
		for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
			txt := s.TokenText()
			if txt[0] == '$' {
				num, err := strconv.Atoi(txt[1:])
				if err != nil {
					return "", fmt.Errorf(`unable to read arg number of %q: %s`, txt, err.Error())
				}
				if len(args) < num {
					return "", fmt.Errorf(`argument position #%d is out of bounds: %d`, num, len(args))
				}
				argMap[num] = fmt.Sprint(args[num-1].Value)
			}
		}

		for old, new := range argMap {
			query = strings.Replace(query, fmt.Sprintf("$%d", old), new, -1)
		}
	}
	return query, nil
}
