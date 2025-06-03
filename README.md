# Über dieses Repository

In diesem Repository zeige ich anhand von Codebeispielen, Aufgaben, dieser README und der Präsentation (presentation.pdf) folgendes:
- typische Probleme, die bei der Entwicklung von Kafka Streams Applikationen aufkommen
- Lösungsansätze
- weiterführende Quellen

# Kurzeinführung

## Kafka Streams
- Kafka Streams ist ein Daten-Streaming-Framework
- Daten-Streaming bedeutet im Wesentlichen, Kafka Messages in Echtzeit verarbeiten
- Kafka Streams Apps lesen von Kafka, verarbeiten die Messages und schreiben wieder nach Kafka
- Skalierbar (sehr große Datenmengen), Fehlertolerant (Redundanz), Exactly Once Processing-Garantie

# Softwaredesign
Motivation:
- Code soll leicht zu verstehen sein
- Code soll Erweiterbar sein
- Code soll gut testbar sein

## Root topology
- Beispiel für Root-Topology: `UserGroupRootTopology.java`
- Hier hat man einen Überblick über die gesamte Topologie incl.
  - Input-Topics
  - Verarbeitungsschritte in Reihenfolge
  - Output-Topics
- einfach unit- und integrationstestbar
- Alternative Möglichkeit: Resultat jedes Verarbeitungsschritts als Spring Bean im Application Context bereitstellen und von dort in den nächsten Verarbeitungsschritt injecten.
  - Vorteil: Noch stärkere Entkopplung wodurch z.B. Einfügen neuer Verarbeitungsschritte leichter wird
  - Nachteil: Schwerer, die Topologie im Code zu erkennen und nachzuvollziehen (potenziell großes Problem bei Kafka Streams)
- Problem der Streams-DSL: Man sieht in der Typisierung (`KStream<String, XXX>`) nicht, was der aktuelle Key ist, was für das Verständnis
  der Implementierung aber essenziell ist ~> per Kommentar dazuschreiben

## Topology Components
- werden von Root-Topology genutzt (`UserGroupRootTopology.java` nutzt die Klassen im `components` Package)
- Implementieren die Verarbeitungsschritte in Kafka Streams DSL oder PAPI
- Leicht Unittestbar (siehe Tests in `src/test/java/.../components`)

# Parallelisierung und Race Conditions

Motivation:
- Kafka Streams als Parallelisierungsframework verstehen
- Verstehen wie Race-Conditions zustande kommen
- Wie kann man sie vermeiden

## Horizontale Skalierung
- "Unit of Parallelism" in Kafka Streams: Topic Partitions und Stream Tasks
- vereinfacht ausgedrückt kann man mit Kafka Streams folgendermaßen horizontal skalieren:
  - Topic mit mehreren Partitionen konfigurieren
    - eine Message mit gegebenem Key wird in die Partition `|murmur2hash(key)| % (numPartitions-1)` gesteckt
    - als Konsequenz landen Messages mit gleichem Key stets in derselben Partition (z.B. ID der User Entity)
    - jede Partition enthält nur einen Teil der Messages des Topics
    - Partitionen können auf verschiedenen Brokern liegen
  - Mehrere Instanzen der Kafka Streams Applikation mit gleicher Consumer-`group.id` starten. Die Partitionen werden gleichmäßig über die Instanzen verteilt.
  - Bekommt eine Instanz mehrere Partitionen zugewiesen, startet sie mehrere unabhängige Stream Task zur parallelen Verarbeitung.
  - So kann man die Rechenlast auf mehrere Rechenknoten aufteilen
- So extrem große Datenmengen in Echtzeit verarbeitbar. Ursprünglicher Use-Case für die Entwicklung von Kafka bei LinkedIn: User Activity auf Website in Echtzeit verarbeiten
- ZWEI OFFENSICHTLICHE PROBLEME:
  - Reihenfolge der Messages ist nur innerhalb einer Partition definiert
  - Was, wenn ich für meine Berechnung Daten von verschiedenen Partitionen brauche?
    - Bsp: User ist in einer Gruppe
    - Möchte eine Berechnung mit der ganzen Gruppe anstellen
    - Alle User mit derselben Gruppe müssen in dieselbe Partition
    - Kafka Streams unterstützt mich dabei (z.B. durch automatisches Re-Partitioning)

## Showcase: Race Conditions

> **AUFGABE**:
> 1. Führe den Test in `RaceConditionIntegrationTest.java` aus und vergewissere dich, dass er grün ist.
> 2. Erhöhe die Anzahl der Partitionen der Input-Topics. Nun sollte der Test fehlschlagen. Warum?

Durch den `RaceConditionIntegrationTest.java` kann man, wenn man für das `user-lists` Topic
mehrere Partitionen konfiguriert, einen Bug feststellen, der durch eine Race Condition zustande kommt.
- Die Race Condition entsteht durch die Flat-Map Operation, die den Key von `grp#` im Topic `user-lists` zu `user#` im Topic `user-to-group-mapping` ändert.
- Hier ist das Problem, dass ein gegebener User, z.B. `user1` die Gruppe wechseln kann, z.B. von `grp1` zu `grp2`. Die Gruppen können im Topic `user-lists` in
  verschiedenen Partitionen liegen, weshalb die Reihenfolge der Records nicht definiert ist. Gehört `user1` also **zuerst** zu `grp1` und wechselt **dann** zu `grp2`, geht diese **zeitliche**
  Information verloren, wenn die Gruppen in verschiedenen Partitionen liegen. Die Flat-Map Operation könnte den `grp2`-Record vor dem `grp1`-Record verarbeiten.
- **Siehe presentation.pdf für eine detaillierte Illustration des Problems!**
- Beachte: im `RaceConditionIntegrationTest.java` und in der Präsentation wird `user1` zunächst von `grp1` entfernt und dann erst `grp2` hinzugefügt. Wird die "Entfernen"-Operation zuletzt verarbeitet, gehört der User zu gar keiner Gruppe mehr und wird aus dem Output-Topic gelöscht.
- Um dieses Problem zu debuggen ist es sehr hilfreich wenn
  - interne Topics sinnvoll benannt sind
  - man den Record Cache ausschaltet (`statestore.cache.max.bytes=0`), damit alle Records, auch die, die Zwischenergebnisse der Verarbeitung darstellen,
    sofort downstream geschickt und in die Kafka Topics geflusht werden und
    nicht durch die Compaction verloren gehen

Race conditions entstehen allgemein bei Key-Änderungen. Darum besondere Vorsicht im Umgang mit:
- `KStream#map()`
- `KStream#flatMap()`
- **Die Frage die man sich stellen muss**: Kommen Messages, die im Output-Topic geordnet sein müssen (z.B. den Zustand einer bestimmten Entity darstellen) aus verschiedenen Partitionen des Input-Topics?

Ansätze um Race Conditions zu vermeiden:
- geeignete Integrationstests schreiben (siehe Abschnitt zu Integrationstests)
- ggf. E2E-Tests implementieren, da Race Conditions auch bei der Interaktion zwischen zwei Streams-Apps entstehen können
- Child-Entities in eigenem Topic speichern, statt sie an Parent-Entities zu hängen
- **Nur solche Berechnungen in Kafka Streams implementieren, die sich leicht mit der Kafka Streams DSL abbilden lassen**
  - Andere Berechnungen können dann vor oder nach der Kafka Streams-Schicht mit geeigneteren Mitteln implementiert werden.
- nur eine Partition verwenden (verhindert horizontale Skalierung)
- geschickte Nutzung von Composite Keys (siehe nächster Abschnitt)

Frage: Warum funktioniert groupBy + aggregate, obwohl hier ebenfalls eine Key-Änderung stattfindet? Der Grund ist,
dass die Reihenfolge in der verschiedene User zur Gruppe hinzugefügt/entfernt werden für uns unerheblich ist.

> **AUFGABE**:
> 1. Als Vorbereitung für diese Aufgabe muss zuerst die Aufgabe im Abschnitt "Internal Topic Naming" bearbeitet werden.
> 2. Kommentiere die `@TestPropertySource` Annotation über der Testklasse, sowie den Code-Block mit den
> `Thread.sleep(...)`-Aufrufen ein. Lies den einkommentierten Code und mach dir klar, was er bewirkt.
> 3. Nutze die Skripte im `scripts`-Ordner (*nur unter Linux getestet*) um die Topics zu inspizieren und herauszufinden,
> wo die fehlenden Gruppen verloren gehen.


### Fix der Race Condition mit Composite Key

- Nutzt man im `user-to-group-mapping`-Topic den Composite Key `grp#-user#`, kann man die Race Condition vermeiden (siehe Topologie mit Composite Key in presentation.pdf)
- Dadurch ändert sich die Semantik des Topics von User X (key) gehört zu Gruppe Y (value) zu: Es existiert eine Assoziation
  zwischen User X und Gruppe Y (beides im Key ausgedrückt). Der Zustand der Existenz oder Nichtexistenz einer gegebenen Assoziation
  wird dann im Topic unter demselben Key gespeichert und die Records für einen gegebenen `grpY-userX` Key werden alle
  aus der Partition von `grpY` im `user-lists` topic generiert.
- Problem:
  - Join mit User-Entities muss jetzt als Foreign-Key-Join implementiert werden. Siehe Präsentation für überarbeitete Topologie
    mit Composite Key
  - Allgemein wird die Topologie komplexer, was sich negativ auf die Wartbarkeit auswirkt

> **AUFGABE**:
> 1. Erstelle eine Abfolge von Grafiken wie in presentation.pdf Abschnitt "Parallelisierung - Race Conditions" um Message für Message nachzuvollziehen,
>    weshalb bei Verwendung des Composite-Keys keine Race Condition mehr zustande kommt
> 2. Implementiere die Topologie mit Composite Keys wie in presentation.pdf dargestellt (Beachte: nicht nur Keys, auch Values sind hier anders)

# Testing

Motivation:
- Warum man Integrationstests braucht
- Wie man sie leicht implementieren kann

## Unit-Tests
Unit-Tests basierend auf `TopologyTestDriver`:
- records durch die Topologie schicken um Logik zu testen
- viele Komponenten der Kafka Streams App fehlen (keine Stream Tasks, Stream Threads, Record Caches, ...)
- ist keine Kafka Cluster-Simulation
- Bugs die mit Parallelisierung zusammen hängen können nicht gefunden werden.

## Integrationstests
- Vorteile Embedded Kafka gegenüber Testcontainer:
  - Embedded Kafka kann einen Cluster mit mehreren Brokern simulieren
  - Es gibt diverse Test-Utils in `spring-kafka-test`, manche spezifisch für Embedded Kafka
- bei Verwendung von `KafkaAvroSerializer/Deserializer` in Tests die Config `schema.registry.url=mock://*` setzen
- Um Bugs durch die Parallelisierung zu finden:
  - erzeuge rasche Abfolge vieler über mehrere Partitionen verteilter Zustandsänderungen
  - im Beispiel `RaceConditionIntegrationTest.java`:
    - erzeuge 1000 Gruppen mit je einem User
    - dann verschiebe jeden User in die jeweils nächste Gruppe
- Nutze für Assertions z.B. `awaitility` um periodisch zu prüfen, ob der Zielzustand
eingetreten ist. Alternativ könnte man versuchen mit den Assertions zu warten, bis keines der Topics (incl. interne) mehr einen Consumer-Lag aufweist, Implementierung ist aber aufwändig.


# Internal Topic Naming

Motivation:
- Verstehen, warum Benennung interner Topics essenziell ist
- Zeigen, wie man sicherstellt, dass alle Topics benannt sind

- Kafka Streams erzeugt interne Topics u.A. für
  - Repartitioning
  - Changelog (Persistenz der Daten eines State Stores)
- Interne Topics müssen explizit benannt werden
- Gründe:
  - Topologie debuggen (z.B. eine erwartete Message ist nicht im Output-Topic. Wo "geht sie verloren"?)
  - Bei Veränderung der Topologie ändert sich die Nummerierung der Topics => State kann nicht geladen werden

Maßnahmen
- Topologie nach unbenannten Topics scannen (`TopicNamingTest.java`)

> **AUFGABE**: Führe `TopicNamingTest.java` aus. Finde die Codestellen wo noch explizite
> Benennung von Topics nötig ist.

Links:
- https://docs.confluent.io/platform/current/streams/developer-guide/dsl-topology-naming.html
- https://www.confluent.io/learn/kafka-topic-naming-convention/

# Weitere Fallstricke

## Data Explosion
### Showcase: Data Explosion

> **AUFGABE**:
> 1. Führe den Test in `DataExplosionIllustration.java` aus: Nur 2 bis 3 Messages werden vom Consumer empfangen.
> 2. Kommentiere die `@TestPropertySource`-Annotation über der Klasse ein und führe den Test erneut aus. Wie viele Messages
>    werden jetzt empfangen? Warum genau diese Anzahl?
> 3. Recherchiere: Was macht das `statestore.cache.max.bytes` property? Welches Feature des Record Caches ist für die unterschiedliche Anzahl Messages im Output-Topic verantwortlich? 

- Anmerkung `statestore.cache.max.bytes=0` soll Probleme mit dem Record Cache simulieren

### Maßnahmen
- Monitoring: records-sent um problem frühzeitig zu erkennen
- record cache auslastung monitoren    
- kartes. Produkte vermeiden
- Dedup
- exactly-once-semantics (ohne gibts duplikate in den output-records)

## Schemas wie AVRO nicht für Keys nutzen
- AVRO-Messages enthalten Metadaten!
- Metadaten des Keys werden bei der Berechnung der Partition mit einbezogen
- Konkretes Szenario: doc-Feld im AVRO-Schema ändern
  => Schema ID ändert sich
  => Partitionierung ändert sich, obwohl Key Payload gleich bleibt

Link: https://forum.confluent.io/t/partitioning-gotchas-dont-use-avro-json-or-protobuf-for-keys-and-be-aware-of-client-hashing-differences/2718

## RecordTooLargeException
- In Kafka ist die maximale Record-Größe beschränkt (Broker: `message.max.bytes`, Kafka-Streams: `max.request.size`)
- Bei groupBy+aggregate ausprobieren oder abschätzen, ob Record zu groß werden kann

# State Stores

Motivation:
- Welche Probleme State Stores mit sich bringen
- Verweis auf Quellen, wo beschrieben wird, wie man die Probleme erkennt und vermeidet

- Stateful Operationen, wie die Erzeugung eines KTable, joins, aggregation, ...
speichern ihren Zustand in State Stores.
- Per Default nutzt Kafka Streams einen RocksDB-State Store, bei dem der Zustand auf Festplatte gespeichert wird

Vorteile
- Die Kafka Streams App kann mehr State speichern, als Memory verfügbar ist

- Hohe Komplexität der State Stores, incl. zahlreicher Settings
- Mögliche Probleme sind z.B.
  - Hoher Festplattenverbrauch
  - Hohe Disc I/O
  - Zu viele File Handles

Maßnahmen:
- Monitoring um Probleme frühzeitig zu erkennen
-  "keep the number of state stores on a single node under 30" (m5.xlarge + EBS in AWS)¹
  - unsere Beispieltopologie hat bereits 5!!!
  - verarbeitet eine App mehrere Partitionen, startet sie auch mehrere Instanzen der Topologie mit eigenen State Stores!!!
- ggf. horizontale Skalierung nötig
- Fine-Tuning über zahlreiche Konfigurationsmöglichkeiten
- Ggf. in-memory-state-stores verwenden, wenn genug Memory zur Verfügung steht (werden ebenfalls zusätzlich in changelog Topic gespeichert)

Weiterführende Links, wo nützliche Metriken, Diagnose und Tuning erläutert werden:
- https://www.responsive.dev/blog/a-size-for-every-stream
- https://www.confluent.io/blog/how-to-tune-rocksdb-kafka-streams-state-stores-performance/
- https://www.responsive.dev/blog/guide-to-kafka-streams-state

# Monitoring und Observability

Motivation:
- Wie man mit OSS und Community-Dashboards Monitoring und Observability angehen kann
- Auf grundlegende Metriken verweisen

Guter OSS-Stack:
- AKHQ als Kafka UI: https://akhq.io/
  - Topics durchsuchen
  - Consumer Groups monitoren
  - incl. Schema Registry UI
  - ...
- Prometheus um Metriken zu aggregieren und für Alerting
- Grafana zur Visualisierung von Metriken
  - In der Community gibt es viele fertige Dashboards, die man nutzen kann https://github.com/kineticedge/kafka-streams-dashboards/tree/e296340cd498f7f29827eb496e3076c191b342ca/monitoring/grafana/dashboards/streams

Wichtige Metriken:
- Kafka Clients https://kafka.apache.org/documentation/#producer_monitoring, https://kafka.apache.org/documentation/#consumer_monitoring
  - `records-lag` (zu hohes Datenaufkommen erkennen)
  - `last-rebalance-seconds-ago` (zuverlässig erkennen ob Consumer healthy ist)
  - `record-send-total` (data explosion erkennen)
- Kafka Streams https://docs.confluent.io/platform/current/streams/monitoring.html#built-in-metrics
  - `task-created-total - task-closed-total` (entspricht die Anzahl Stream Tasks den Erwartungen)
  - `commit-ratio` `poll-ratio` `process-ratio` (schauen, womit die App gerade beschäftigt ist)
  - `blocked-time-ns-total` (schauen, ob die Applikation auf Kafka wartet, z.B. wegen Rate limiting)
  - `cache-size-bytes-total` (seit Version 3.4, per Stream Task; schauen ob der record cache voll ist, was zu data explosion führen kann)
  - `hit-ratio` (record cache hit ratio. Indikator ob record cache zu klein ist)
- RocksDB (erfordert teils metrics DEBUG level) https://docs.confluent.io/platform/current/streams/monitoring.html#rocksdb-metrics
  - `block-cache-data-hit-ratio`, `block-cache-index-hit-ratio`, `block-cache-filter-hit-ratio`, `memtable-hit-ratio` (RockDB's memory caches, die Disc I/O vermeiden sollen)
  - `total-sst-files-size` (vom State Store eingenommener Platz auf der Festplatte)
  - `write-stall-duration` (Write Stalls erkennen)
  - Zusätzliche Diagnose-Tools:
    - `free -mh` (hit ratio des OS disc blocks cache, der Disc I/O vermeiden soll)
    - `iostat` (disc IOPS)

Links:
- https://www.responsive.dev/blog/monitoring-kafka-streams
- https://www.responsive.dev/blog/a-size-for-every-stream


# Integration mit angrenzenden Systemen

- Kafka Connect ist ein mächtiges Tool zur Integration von Kafka mit externen Systemen
- Source- und Sink-Connectors ermöglichen Import/Export von Daten
- großes Ökosystem von fertigen Connectors z.B. für
  - Relationale Datenbanken
  - S3
  - Elasticsearch
  - ...
- Bewährtes Pattern, wenn man eine Datenbanktabelle und ein Kafka-Topic in Sync halten will: Outbox Pattern https://microservices.io/patterns/data/transactional-outbox.html

# Fazit

Kafka Streams ist komplex, deshalb:
- Anforderungen klären:
    - Welche Features nutzen wir, um welche Anforderung zu erfüllen?
    - Performance-Anforderungen spezifizieren
- Nur den Teil der Applikation in Kafka Streams implementieren, der diese Features benötigt
- Realistische Integrationstests und ggf. E2E-Tests schreiben
- Von Anfang an Monitoring & Observability implementieren
