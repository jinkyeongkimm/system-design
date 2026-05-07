# Apache Kafka

> 참고: [How Kafka Works](https://newsletter.systemdesign.one/p/how-kafka-works)
//
---

## 1. 도입 배경

LinkedIn은 서비스 수가 늘어나면서 서비스 간 데이터를 점대점(point-to-point)으로 연결했고, 통합 경로가 O(N²)으로 폭발적으로 증가했다. Kafka는 이를 해결하기 위해 **중앙화된 데이터 허브** 역할을 한다. 데이터를 한 곳에 저장하면 여러 시스템이 독립적으로 소비할 수 있어 통합 복잡도가 O(N)으로 줄어든다.

---

## 2. 요약

### Kafka's Story
LinkedIn이 2010년 데이터 통합 문제를 해결하기 위해 개발한 오픈소스 분산 메시징 시스템. 현재는 Apache 재단 프로젝트로, 메시징·스트림 처리·ETL 등 다양한 용도로 사용된다.

### The Basic Kafka Concepts

#### The Log Data Structure
Kafka의 모든 저장은 **Append-only Log** 위에서 동작한다.
- 쓰기는 항상 끝에 추가(수정/삭제 없음) → O(1) 쓰기, lock-free
- 각 레코드는 단조 증가하는 고유 **offset**을 가짐
- 순차 I/O이므로 HDD에서도 높은 처리량을 낸다

#### Records
메시지의 기본 단위.
- `key: byte[]` (선택, 파티션 라우팅에 사용) + `value: byte[]`
- Kafka 자체는 스키마를 모름 → 직렬화/역직렬화는 클라이언트 책임

#### Topics & Partitions

| 개념 | 설명 |
|------|------|
| **Topic** | 데이터 카테고리 (DB의 테이블과 유사) |
| **Partition** | Topic을 샤딩한 단위, 각각 독립된 Log |

- 파티션 덕분에 병렬 처리와 수평 확장이 가능
- 순서는 **파티션 내에서만** 보장

#### Clients & The API
- **Producer**: 메시지를 특정 Topic에 씀
- **Consumer**: Topic을 구독해 메시지를 읽음
- **Admin Client**: 클러스터 메타데이터 관리 (토픽 생성 등)

---

### Kafka as a Distributed System

#### Brokers
클러스터를 구성하는 서버 노드.
- 최소 3개 이상 운영
- 각 파티션은 replication factor(기본 3)만큼 여러 Broker에 복제됨
- 가용 영역(AZ)에 분산 배치해 장애 내성 확보

#### Scalability
- Append-only 특성으로 O(1) 쓰기 유지
- Broker를 추가하면 파티션을 분산해 **선형 확장** 가능
- 이론상 처리량을 2배로 늘리려면 Broker를 2배로 늘리면 됨

#### Replication
- 각 파티션은 Leader 1개 + Follower N개로 구성
- Follower는 Leader를 복제하며 hot standby 상태 유지
- Broker 장애 시 Follower가 자동으로 Leader로 승격

#### Leaders
- **쓰기**: 항상 Leader에서만 수행
- **읽기**: 기본은 Leader, KIP-392(Kafka 2.4+)부터 가장 가까운 Follower에서 읽기 가능 (`replica.selector.class` 설정)

> **왜 기본은 Leader 읽기인가?**  
> Follower는 Leader와 동기화 지연(lag)이 있어 stale read가 발생할 수 있다. 일관성을 기본으로 보장하되, 멀티 AZ 환경에서 지연시간·네트워크 비용을 줄이고 싶을 때 Follower 읽기를 선택적으로 활성화하는 구조다.

#### The Metadata Log
클러스터 상태 변경(리더 선출, 토픽 생성 등)을 일반 Kafka 토픽과 동일한 방식(Log)으로 저장하는 특수 토픽(`__cluster_metadata`).
- 이벤트 순서 보장
- Log를 순서대로 재생하면 항상 동일한 상태로 복원 가능 (결정적 재구성)
- Log의 내구성을 그대로 상속

#### Controllers
제어 평면(control plane) 담당.
- Active Controller 1개 + Standby 2개
- 파티션 리더 지정, 토픽 생성, 설정 변경 담당
- Broker 생존성 감지 (Heartbeat 없으면 격리)

#### KRaft (Kafka Raft)
Raft에서 영감을 받은 Kafka 자체 합의 알고리즘.
- Controller 간 리더 선출에 사용
- 메타데이터 로그 커밋 시 과반 쿼럼 필요

**리더 선출 2단계:**
1. KRaft로 Controller 간 Active Controller 선출
2. Active Controller가 각 파티션 리더 지정

> **왜 ZooKeeper에서 KRaft로 바꿨나?**  
> ZooKeeper는 외부 의존성으로 운영 복잡도가 높았다. KRaft로 메타데이터를 Kafka 내부로 통합해 배포와 운영을 단순화했다.

---

### Other Features That Set Kafka Apart

#### Data Retention
Consumer가 메시지를 읽어도 즉시 삭제하지 않고, 설정한 기간(예: 7일) 동안 보관한다.

> **왜 이렇게 결정했나?**  
> 기존 메시지 큐는 소비 후 삭제해 재처리가 불가능했다. Kafka는 보존 기간 내 언제든 재처리(버그 수정 후 과거 이벤트 재실행 등)를 허용한다. 저장 비용이 늘어나는 트레이드오프가 있다.

#### Tiered Storage
```
최근 데이터 → Broker 디스크 (SSD, 고성능)
오래된 데이터 → S3 등 오브젝트 스토리지 (저비용)
```

> **왜 이렇게 결정했나?**  
> 모든 데이터를 Broker 디스크에 두면 장기 보존 비용이 급증하고, 새 Broker 추가 시 데이터 이동 시간이 길어진다. S3는 HDD 대비 10배 이상 저렴하고, 신규 Broker가 S3에서 직접 데이터를 로드해 클러스터 탄력성도 높아진다.

- 비용 10배 이상 절감
- 클러스터 탄력성 향상
- 핫 데이터 지연시간은 유지, 콜드 데이터 지연시간은 미미하게 증가

#### Consumer Groups & Read Parallelization
```
Topic (파티션 3개)
  Partition 0 → Consumer A
  Partition 1 → Consumer B  } Consumer Group 1
  Partition 2 → Consumer C

  Partition 0~2 → Consumer D  } Consumer Group 2 (독립 소비)
```

- 1개 파티션은 동일 그룹 내 1개 Consumer만 담당 → 파티션 내 순서 보장
- Consumer 추가 시 처리량 선형 증가 (파티션 수가 상한선)
- 여러 Consumer Group이 같은 Topic을 독립적으로 소비 → 한 번 저장으로 다수 목적에 활용

##### The Consumer Group Membership Protocol
- Group Coordinator(Broker)가 각 Consumer의 파티션 할당 관리
- 진행 상황을 `__consumer_offsets` 토픽에 `{partition → offset}` 형태로 저장
- Heartbeat 기반 생존성 감지, 장애 시 해당 offset부터 재개
- 모든 Broker가 Coordinator 역할 가능 → 핫스팟 없음

#### Transactions & Exactly Once Processing
- 다중 파티션에 **원자적 쓰기** 지원 (2단계 커밋)
- Producer ID + epoch로 재시도 시 중복 방지 (Idempotency)

| 범위 | 보장 수준 |
|------|---------|
| Kafka 내부 | Exactly-once 완벽 보장 |
| 외부 시스템 포함 | 엣지 케이스 존재 |

---

### Other Kafka Components

#### Kafka Streams
- 고수준 클라이언트 라이브러리
- `입력 Topic → 가공(filter, map, join, aggregation) → 출력 Topic`
- Exactly-once 보장 (내부적으로 트랜잭션 사용)
- Windowed aggregation 지원

#### Schema Registry
Kafka는 raw bytes만 저장하므로, 별도 HTTP 서비스가 `{스키마, Topic}` 매핑을 관리한다.
```
Producer: 스키마 등록 → 메시지에 스키마 ID 삽입 → 직렬화 후 전송
Consumer: 메시지 수신 → 스키마 ID 추출 → 레지스트리 조회 → 역직렬화
```

#### Kafka Connect
외부 시스템 ↔ Kafka 간 데이터 이동을 코드 없이 처리.
- **Source Connector**: 외부 DB → Kafka
- **Sink Connector**: Kafka → ElasticSearch, Snowflake 등
- REST API로 관리, Consumer Group 프로토콜 기반으로 분산 처리

---

## 3. 핵심 패턴

| 패턴 | Kafka에서의 적용 | 다른 시스템 활용 예 |
|------|----------------|------------------|
| **Append-only Log** | 모든 저장의 기반. 수정 없이 추가만 해서 O(1) 쓰기와 결정적 재구성 달성 | DB WAL, Git commit history |
| **Event Sourcing** | 상태가 아닌 이벤트를 저장하고 재생으로 상태 복원 (`__cluster_metadata`) | CQRS 아키텍처, 감사 로그 |
| **Sharding (Partitioning)** | 파티션으로 데이터를 분산해 병렬 처리와 선형 확장 달성 | DynamoDB, Cassandra, Elasticsearch |
| **Leader-Follower Replication** | 단순한 일관성 모델로 고가용성 달성. 쓰기는 Leader, 읽기는 선택적으로 Follower 허용 | MySQL replica, Redis Sentinel |
| **Tiered Storage (Hot/Cold 분리)** | 최근 데이터는 고성능 디스크, 오래된 데이터는 저비용 오브젝트 스토리지로 분리 | Elasticsearch ILM, Cassandra TWCS |
| **Quorum-based Consensus** | 과반 동의로 분산 환경에서 데이터 커밋 보장 (KRaft) | Zookeeper ZAB, etcd Raft |
| **Idempotency** | Producer ID + epoch로 재시도 시 중복 방지 | HTTP 멱등 API, DB upsert |
| **2-Phase Commit** | Exactly-once를 위한 원자적 다중 파티션 쓰기 | 분산 트랜잭션, XA Protocol |
