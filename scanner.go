package parser

import (
	"bufio"
	"io"
)

// Scanner is a stream oriented parser for W3C Extended Log Format lines.
type Scanner struct {
	reader  io.Reader
	strings []string
	done    bool
	buf     []byte
	origbuf []byte
	err     error
}

// NewScanner constructs a Scanner.
func NewScanner(reader io.Reader) *Scanner {
	s := Scanner{
		reader:  reader,
		origbuf: make([]byte, 0, 65536),
	}
	s.buf = s.origbuf
	return &s
}

// Scan advances the Scanner to the next log line, which will then be available
// through the Strings method. It returns false when the scan stops, either by
// reaching the end of the input or an error. After Scan returns false, the Err
// method will return any error that occurred during scanning, except that if
// it was io.EOF, Err will return nil.
func (s *Scanner) Scan() bool {
	if s.done {
		return false
	}
	var err error
	var rest []byte
	var strings []string
	var n int
	for {
		if s.err != nil && s.err != io.EOF {
			return false
		}
		if len(s.buf) > 0 {
			// try to parse what we have in buf
			rest, strings, err = ExtractStrings(s.buf)
			if err == nil {
				if len(strings) > 0 {
					// we got a log line
					s.strings = strings
					s.buf = rest
					return true
				}
				// there was no content that could be extracted
				// so we need more data, just get rid of the useless spaces
				s.buf = rest
			} else if err != ErrNoEndline && err != ErrQuoteLeftOpen {
				// parsing error
				s.err = err
				return false
			} else if s.err == io.EOF && err == ErrNoEndline {
				// there is no more available data to read
				// just output the last content
				if len(strings) > 0 {
					s.strings = strings
					s.buf = rest
					return true
				}
				return false
			} else if s.err == io.EOF && err == ErrQuoteLeftOpen {
				// there is no more available data to read
				// but the last content is not valid
				s.err = err
				return false
			}
			// here, at the end of the if/elseif, we know that err is a
			// "incomplete line" error, and that we can try to read more data
		}
		// there was not enough data to generate new content
		if s.err != nil {
			return false
		}
		// if there is no more space on the right side of s.buf, or if there is
		// much space on the left side of s.buf, then copy the data to the
		// beginning of s.origbuf
		if cap(s.buf) < 65536 && (len(s.buf) == cap(s.buf) || cap(s.buf) < 32768) {
			copy(s.origbuf[:len(s.buf)], s.buf)
			s.buf = s.origbuf[:len(s.buf)]
		}
		if len(s.buf) == 65536 {
			// the line to parse is too long
			s.err = bufio.ErrTooLong
			return false
		}
		// read some more data into the free space on the right side of s.buf
		n, err = s.reader.Read(s.buf[len(s.buf):cap(s.buf)])
		if err == io.EOF {
			if s.err == nil {
				s.err = io.EOF
			}
		} else if err != nil {
			s.err = err
			return false
		} else if n == 0 {
			// err == nil but n == 0
			s.err = io.ErrNoProgress
			return false
		}
		s.buf = s.buf[:len(s.buf)+n]
	}
}

// Strings returns the most recent fields generated by a call to Scan as a newly allocated string slice.
func (s *Scanner) Strings() []string {
	return s.strings
}

// Err returns the first non-EOF error that was encountered by the Scanner.
func (s *Scanner) Err() error {
	if s.err != nil && s.err != io.EOF {
		return s.err
	}
	return nil
}
