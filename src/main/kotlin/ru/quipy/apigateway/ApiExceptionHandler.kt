package ru.quipy.apigateway

import io.github.resilience4j.ratelimiter.RequestNotPermitted
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(RequestNotPermitted::class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    fun handleRequestNotPermitted() = Unit
}
