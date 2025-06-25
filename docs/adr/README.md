# Architecture Decision Records (ADR)

This directory contains Architecture Decision Records for the SQLModel Medallion Demo platform.

## ADR Index

| ADR | Title | Status | Date | Impact |
|-----|-------|--------|------|--------|
| [ADR-001](./ADR-001-spark-4-type-safety.md) | Spark 4.0.0 Type Safety Requirements | ✅ Accepted | 2025-06-25 | Critical |
| [ADR-002](./ADR-002-individual-customer-metrics.md) | Individual Customer Metrics Design | ✅ Accepted | 2025-06-25 | High |
| [ADR-003](./ADR-003-strict-data-lineage.md) | Strict Data Lineage Enforcement | ✅ Accepted | 2025-06-25 | Critical |
| [ADR-004](./ADR-004-fail-fast-error-handling.md) | Fail-Fast Error Handling Strategy | ✅ Accepted | 2025-06-25 | High |
| [ADR-005](./ADR-005-evolution-tracking-system.md) | Platform Evolution Tracking System | ✅ Accepted | 2025-06-25 | Medium |

## Status Definitions

- **✅ Accepted**: Decision is approved and implemented
- **🔄 Proposed**: Decision is under review
- **⚠️ Deprecated**: Decision is no longer recommended
- **❌ Rejected**: Decision was considered but not adopted

## ADR Process

### When to Create an ADR
- Any architectural decision that affects multiple layers/components
- Technology choices that impact platform direction
- Design patterns that will be reused across the platform
- Breaking changes that affect external interfaces
- Performance or scalability decisions with long-term impact

### ADR Template
Use the template in `ADR-TEMPLATE.md` for consistency.

### Review Process
1. **Draft**: Create ADR with "Proposed" status
2. **Review**: Team review and discussion
3. **Decision**: Mark as "Accepted", "Rejected", or "Deprecated"
4. **Implementation**: Execute the decision
5. **Update**: Maintain ADR as implementation evolves

## Platform Impact Classification

- **Critical**: Affects core platform architecture or data integrity
- **High**: Significant impact on performance, scalability, or user experience
- **Medium**: Important design decisions with moderate platform impact
- **Low**: Local decisions with minimal platform-wide effects

---

*ADRs capture the "why" behind our architectural decisions, ensuring institutional knowledge persists as the platform evolves.*