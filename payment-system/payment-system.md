# 결제 시스템 설계 — Uber는 어떻게 하루 3천만 건의 거래를 처리하는가

---

## 1. 도입 배경

### 해결해야 할 문제

Uber는 하루 약 1,500만 건의 운행이 발생하며, 운행 1건당 최소 2번 이상의 트랜잭션(선승인 + 실결제)이 필요하다. 즉, 하루 **3천만 건 이상의 결제 트랜잭션**을 처리해야 한다.

결제 시스템은 세 가지 근본적인 어려움을 갖는다.

| 문제 | 내용 |
|------|------|
| **보안(Security)** | 신용카드 정보를 모바일 기기나 Uber 서버에 직접 저장하면 도난·해킹 위험이 크고, 법적 컴플라이언스 부담도 막대함 |
| **분배(Disbursement)** | 단일 결제금액을 드라이버 수익 / 플랫폼 수수료 / 세금·기타 비용으로 분리해야 하므로 단순 이체가 불가능 |
| **신뢰성(Reliability)** | 은행·카드 네트워크·결제 제공업체 등 외부 시스템에 의존하기 때문에 네트워크 장애, 타임아웃, 부분 중단 리스크가 항상 존재 |

---

## 2. 요약

### 2-1. 결제 정보 등록 — 토큰화(Tokenization)

```
[모바일 앱]
    │  신용카드 번호 + CVV 입력
    ▼
[Payment Provider SDK]   ← Uber 서버를 거치지 않음
    │  카드 정보 + 메타데이터(Uber 계정, 결제 수단 유형 등) 전송
    ▼
[Payment Provider]
    │  고유 토큰(Token) 반환
    ▼
[모바일 앱]  ← 이후 토큰만 사용
```

- SDK가 카드 정보를 **Uber 서버가 아닌 결제 제공업체에 직접** 전송한다.
- 결제 제공업체(Adyen, Stripe, Braintree 등)는 카드를 대리하는 **고유 토큰**을 반환한다.
- 토큰은 Uber 계정·앱에 바인딩되어 **다른 앱·기기에서는 사용 불가**하므로 탈취해도 무용지물이다.
- 결과적으로 Uber 서버나 모바일 기기에는 **민감한 카드 정보가 존재하지 않는다**.

---

### 2-2. 승차 요청 — 사전 승인(Pre-Authorization)

```
[모바일 앱] → [Uber 백엔드] → [Payment Service]
                                    │ 리스크 체크 (도난 카드 DB, 속도 체크 등)
                                    │ Payment Provider에 인증
                                    ▼
                              [Payment Provider]
                                    │ 토큰 유효성 검증
                                    ▼
                              [Card Network (Visa/Mastercard)]
                                    ▼
                              [은행] → 임시 자금 홀드(Hold)
                                    │ Transaction ID 반환
                                    ▼
                              [Uber 백엔드] ← 결과 수신
```

- Payment Service는 **리스크 체크** 후 토큰을 Payment Provider에 전달해 예상 요금만큼 **사전 승인**을 요청한다.
- 은행은 실제 이체가 아닌 **임시 자금 홀드**를 걸고, 사용자 계좌에는 "대기 중인 청구"가 표시된다.
- 운행이 취소되면 Payment Service가 홀드 해제를 요청한다.
- Payment Provider는 **Transaction ID**를 반환하며, 이후 상태 조회·취소에 활용된다.

---

### 2-3. 운행 완료 — 결제 실행 (Kafka 기반 이벤트 흐름)

```
[Trip Service]  → 최종 요금 계산 → '비즈니스 이벤트' 발행
        ▼
[Payment Service]
  - Payment Order 생성 (고유 ID + 상태: NOT_STARTED)
  - Payments DB(Payment Orders 테이블)에 저장
  - Kafka에 payment_order 이벤트 발행
        ▼
[Order Processor]
  - 이벤트 소비
  - 드라이버 수익 / 플랫폼 수수료 / 세금 분리 계산
  - Kafka에 결제 "Intent" 발행 (결정 영속화)
        ▼
[Order Processor Worker]
  - Intent 소비
  - Payment Provider 호출 (실제 청구)
  - Payment Service에 상태 업데이트 요청: EXECUTING
        ▼
[Payment Provider]
  - 동기 API 응답 또는 비동기 웹훅(Webhook)으로 결과 전달
        ▼
[Order Processor]
  - 결과를 Kafka에 기록
  - Payment Service에 최종 상태 업데이트 요청: SUCCESS / FAILED
```

**Payment Orders 테이블 스키마**

| 필드 | 설명 |
|------|------|
| `order_id` | 고유 주문 ID |
| `trip_id` | 연결된 운행 ID |
| `amount` | 결제 금액 |
| `status` | NOT_STARTED → EXECUTING → SUCCESS / FAILED |
| `idempotency_key` | 중복 결제 방지 키 |
| `txn_id` | Payment Provider가 발행한 Transaction ID |
| `timestamp` | 생성 시각 |

> **핵심**: 모든 참여자(드라이버, 플랫폼 등)가 성공적으로 처리된 경우에만 status를 SUCCESS로 업데이트한다.

---

### 2-4. 결제 분배 — 자금 이동

```
[홀드 → 실결제 전환]
  - final fare < hold  →  초과분 홀드 해제
  - final fare > hold  →  추가 승인 요청

[자금 이동 흐름]
  라이더 은행 계좌
      ↓ 운행 완료 후
  Uber 플랫폼 계좌  (Uber가 플랫폼 수수료, 세금 등 정산)
      ↓
  드라이버 은행 계좌  (Payment Provider 호출)
```

---

### 2-5. 데이터베이스 — 3개의 역할 분리

| DB | 역할 | 핵심 질문 |
|----|------|-----------|
| **Ledger(원장)** | 이중 장부(Double-Entry) 방식의 불변 금융 기록 / 모든 트랜잭션 합계 = 0 / NoSQL 기반 커스텀 DB | 재무적 진실은? 누가 무엇을 빚지고 있나? 감사 가능한가? |
| **Wallet(지갑)** | Ledger에서 파생된 집계 잔액 뷰 / 드라이버 수익·플랫폼 잔액·라이더 지불 이력 저장 | 지금 가용 잔액은? 지금 출금 가능한가? |
| **Payments DB** | 결제 주문과 실행 상태를 추적하는 OLTP(SQL) DB | 결제가 발생했나? 성공했나? 재시도해도 안전한가? |

> 3개 DB를 분리하는 이유: 합치면 감사 신뢰성이 떨어지고, 부분 실패 시 복구가 어렵다.

---

### 2-6. 보안 체크리스트

- 전송 중 데이터 암호화 (데이터 변조 방지)
- 신용카드 정보 토큰화
- HTTPS(TLS) 적용 (도청 방지)
- DB 복제 및 데이터 백업 (데이터 손실 방지)
- 요청 속도 제한(Rate Limit) + 방화벽 (DDoS 방지)
- TLS + 인증서 고정(Certificate Pinning) (중간자 공격 방지)
- Payment Provider 웹훅 검증 (가짜 결제 상태 업데이트 방지)

---

## 3. 핵심 패턴

### 패턴 1: 토큰화 (Tokenization)
> 민감 데이터를 직접 저장하지 않고, 제3자가 발급한 토큰으로 대체한다.

- **적용 사례**: Uber(카드 정보), AWS S3(임시 자격증명), OAuth(Access Token)
- **효과**: 핵심 시스템이 민감 데이터를 보관하지 않아 침해 영향 범위를 대폭 축소

### 패턴 2: 사전 승인 + 홀드 (Pre-Authorization Hold)
> 실제 자금 이동 없이 자원을 예약한다.

- **적용 사례**: 결제 시스템(카드 홀드), 재고 시스템(예약 재고), 예약 플랫폼(객실 홀드)
- **효과**: 취소·변경이 빈번한 환경에서 최종 확정 전까지 자원을 안전하게 선점

### 패턴 3: Intent 패턴 (Write-Ahead Intent)
> 외부 시스템 호출 전에 "처리하기로 결정했다"는 기록을 먼저 영속화한다.

- **적용 사례**: 결제 실행(Kafka에 Intent 발행), WAL(Write-Ahead Log), 분산 사가(Saga) 패턴
- **효과**: 네트워크 장애가 발생해도 Intent를 기준으로 재시도 가능, 중복 실행 방지

### 패턴 4: 멱등성 키 (Idempotency Key)
> 동일한 요청이 여러 번 실행되어도 결과가 한 번만 반영되도록 보장한다.

- **적용 사례**: 결제(중복 청구 방지), API 재시도, 메시지 큐 소비
- **효과**: 재시도가 안전해져서 신뢰성 있는 At-Least-Once 처리 구현 가능

### 패턴 5: 이중 장부 (Double-Entry Bookkeeping)
> 모든 금융 이벤트를 차변(Debit)과 대변(Credit) 쌍으로 기록하여 합계가 항상 0이 되도록 한다.

- **적용 사례**: Uber Ledger, Stripe Ledger, 모든 회계 시스템
- **효과**: 금융 감사 가능성(Auditability) 보장, 부분 실패 감지 및 불일치 추적 가능

### 패턴 6: 역할 분리된 데이터 저장소 (Polyglot Persistence by Concern)
> 질문의 성격에 따라 DB를 분리한다 (Ledger / Wallet / Payment Orders).

- **적용 사례**: CQRS(Command Query Responsibility Segregation), 이벤트 소싱
- **효과**: 각 저장소가 단일 책임을 가지므로 감사, 잔액 조회, 실행 상태 추적이 독립적으로 정확하게 동작

---

## 참고 자료

- [원본 뉴스레터 - Payment System Design (Part 1)](https://newsletter.systemdesign.one/p/payment-system-design)
- [Uber - LedgerStore: Trillions of Indexes](https://www.uber.com/en-CA/blog/how-ledgerstore-supports-trillions-of-indexes/)
- [Uber - Money at Scale: Strong Data](https://www.uber.com/en-CA/blog/money-scale-strong-data/)
- [Reliable Processing in a Streaming Payment System](https://www.uber.com/en-CA/blog/)
- [Stripe - Ledger System for Tracking Money Movement](https://stripe.com/blog/ledger-stripe-system-for-tracking-and-validating-money-movement)
