package raft

// empty log struct to be implemented
type logEntry struct {
	Command interface{}
	Term    int
}

type Log struct {
	entrys []logEntry
	index0 int
}

func (l *Log) start() int{
	return l.index0
}

func makeLog(entry []logEntry,index0 int) Log{
	return Log{
		entrys: entry,
		index0: index0,
	}
}
func makeEmptyLog() Log{
	return Log{
		make([]logEntry,1),
		0,
	}
}

func (l *Log) append(entry logEntry)  {
	l.entrys=append(l.entrys,entry)
}

func (l *Log) cutend(index int)  {
	l.entrys = l.entrys[0:index-l.index0]
}

func (l *Log) cutstart(index int)  {
	l.index0 += index
	l.entrys = l.entrys[index:]
}

func (l *Log) slice(index int)  []logEntry{
	return l.entrys[index-l.index0:]
}

func (l *Log) lastIndex()  int{
	return l.index0 + len(l.entrys) -1
}

func (l *Log) entry(index int) *logEntry{
	return &(l.entrys[index-l.index0])
}

func (l *Log) lastentry() *logEntry{
	return l.entry(l.lastIndex())
}