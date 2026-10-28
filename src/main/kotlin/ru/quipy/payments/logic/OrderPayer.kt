package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val QUEUE_CAPACITY = 42
    }

    private class PaymentTask(val deadline: Long, private val action: () -> Unit) : Runnable, Comparable<PaymentTask> {
        override fun run() = action()
        override fun compareTo(other: PaymentTask): Int = deadline.compareTo(other.deadline)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var accounts: List<PaymentExternalSystemAdapter>

    private val account by lazy { accounts.first().properties() }

    private val worstProcessingMillis by lazy { 2 * account.averageProcessingTime.toMillis() }

    private val throughputPerSec by lazy {
        minOf(account.rateLimitPerSec.toDouble(), account.parallelRequests * 1000.0 / worstProcessingMillis)
    }

    private val inFlight by lazy {
        minOf(account.parallelRequests.toLong(), account.rateLimitPerSec * account.averageProcessingTime.toMillis() / 1000).toInt().coerceAtLeast(1)
    }

    private val retryAfterMillis by lazy {
        ceil(1000.0 / minOf(account.rateLimitPerSec.toDouble(), account.parallelRequests * 1000.0 / account.averageProcessingTime.toMillis())).toLong()
    }

    private val paymentsInProgress = AtomicInteger(0)

    private val queue = PriorityBlockingQueue<Runnable>(QUEUE_CAPACITY)

    private val paymentExecutor by lazy {
        ThreadPoolExecutor(
            inFlight,
            inFlight,
            0L,
            TimeUnit.MILLISECONDS,
            queue,
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!takePlaceIfInTime(deadline)) {
            throw PaymentQueueFullException((retryAfterMillis + 999) / 1000)
        }
        val createdAt = System.currentTimeMillis()
        paymentExecutor.execute(PaymentTask(deadline) {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                paymentsInProgress.decrementAndGet()
            }
        })
        return createdAt
    }

    private fun takePlaceIfInTime(deadline: Long): Boolean {
        while (true) {
            val inProgress = paymentsInProgress.get()
            val waiting = maxOf(0, inProgress + 1 - inFlight)
            val waitMillis = (waiting * 1000 / throughputPerSec).toLong()
            if (System.currentTimeMillis() + waitMillis + worstProcessingMillis > deadline) return false
            if (paymentsInProgress.compareAndSet(inProgress, inProgress + 1)) return true
        }
    }
}
