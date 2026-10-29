package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val PAYMENT_EXECUTOR_THREADS = 128
    }

    private class PaymentTask(val deadline: Long, private val action: () -> Unit) : Runnable, Comparable<PaymentTask> {
        override fun run() = action()
        override fun compareTo(other: PaymentTask): Int = deadline.compareTo(other.deadline)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val queue = PriorityBlockingQueue<Runnable>()
    private val inFlight = AtomicInteger()

    private val paymentExecutor = ThreadPoolExecutor(
        PAYMENT_EXECUTOR_THREADS,
        PAYMENT_EXECUTOR_THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        queue,
        NamedThreadFactory("payment-submission-executor")
    )

    private fun tryReserveSlot(deadline: Long): Boolean {
        while (true) {
            val current = inFlight.get()
            val capacity = paymentService.capacity(deadline - System.currentTimeMillis(), PAYMENT_EXECUTOR_THREADS)
            if (current >= capacity) return false
            if (inFlight.compareAndSet(current, current + 1)) return true
        }
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!tryReserveSlot(deadline)) {
            throw PaymentQueueFullException()
        }
        val createdAt = System.currentTimeMillis()
        try {
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
                    inFlight.decrementAndGet()
                }
            })
        } catch (e: RuntimeException) {
            inFlight.decrementAndGet()
            throw e
        }
        return createdAt
    }
}
