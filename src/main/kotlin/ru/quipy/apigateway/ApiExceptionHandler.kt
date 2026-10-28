package ru.quipy.apigateway

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.quipy.payments.logic.PaymentQueueFullException

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(PaymentQueueFullException::class)
    fun handlePaymentQueueFull(e: PaymentQueueFullException): ResponseEntity<Void> =
        ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, e.retryAfter.toString())
            .build()
}
