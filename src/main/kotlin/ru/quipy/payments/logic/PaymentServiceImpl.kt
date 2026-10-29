package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val paymentMetrics: PaymentMetrics,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        private const val IN_FLIGHT_FACTOR = 0.7
    }

    override fun capacity(remainingMs: Long, executorThreads: Int): Int {
        if (remainingMs <= 0 || paymentAccounts.isEmpty()) return 0

        val throughputPerMs = paymentAccounts.minOf { it.throughputPerMs() }
        val avgProcessingMs = paymentAccounts.sumOf { it.averageProcessingMs() }
        if (remainingMs < avgProcessingMs) return 0

        val accountCapacity = throughputPerMs * remainingMs
        val executorCapacity = executorThreads.toDouble() * remainingMs / avgProcessingMs
        return (minOf(accountCapacity, executorCapacity) * IN_FLIGHT_FACTOR).toInt()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            for (account in paymentAccounts) {
                account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }
        } finally {
            paymentMetrics.observeRequestDuration(System.currentTimeMillis() - paymentStartedAt)
        }
    }
}
