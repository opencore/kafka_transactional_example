# kafka_transactional_example
Example of producing data to Kafka and converting from XML to Avro in a transactional way

## Klassen

### SendFuture
Liest eine Textdatei ein in der XML Nachrichten (eine pro Zeile) abgelegt sind und schreibt diese als String in ein Kafka Topic. Es wird hierbei die Methode zum Senden verwendet bei der
Futures gespeichert und jeweils nach einem Batch überprüft werden.
Diese Methode hat manchmal Vorteile, verlangt aber auch, dass der sendede Code ein Konzept von Batches hat nach denen jeweils auf Senden gewartet werden kann.

**Kommandozeilenparameter:**
1. Topic in das Nachrichten produziert werden sollen
2. Datei aus der XML Nachrichten gelesen werden sollen


### SendCallback
Liest eine Textdatei ein in der XML Nachrichten (eine pro Zeile) abgelegt sind und schreibt diese als String in ein Kafka Topic. Es werden hierbei Callbacks zur Kontrolle des Erfolgs verwendet.
Der Vorteil hier ist, dass der Code nicht explizit nach Batches warten muß, sondern nur eine Liste von Exceptions abgefragt werden muß, was eine sehr günstige Operation ist und beim Senden in-line
erledigt werden kann.

In diesem Beispiel wird einmal am Ende auf Exceptions geprüft, für den endgültigen Code würde dieser Check in der Senden Routine eingebaut werden.

**Kommandozeilenparameter:**
1. Topic in das Nachrichten produziert werden sollen
2. Datei aus der XML Nachrichten gelesen werden sollen



### Converter
Diese Klasse liest die von den Sendern geschriebenen XML Nachrichten und versucht diese in ein Avro Schema zu konvertieren. Das Avro Schema wird für diesen PoC aus einer Datei gelesen (resources/avro/Flug.avsc).
Um Exactly Once zu ermöglichen verwendet der Producer Transaktionen und committed als Teil der Transaktion ebenfalls die Consumer Offsets.

Der XML Code in dieser Klasse ist nur als Beispiel gedacht und sollte durch den bestehenden XML Verarbeitungs Code ersetzt werden.

**Kommandozeilenparameter:**
1. Topic aus dem XML Nachrichten gelesen werden sollen
2. Topic in das Avro Nachrichten geschrieben werden sollen

