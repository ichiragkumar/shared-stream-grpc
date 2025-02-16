import { Injectable, LoggerService as NestLoggerService } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import * as winston from 'winston';
import { LoggingWinston } from '@google-cloud/logging-winston';

interface RequestMetrics {
  requestId: string;
  method: string;
  startTime: number;
  userAgent: string;
  ipAddress: string;
  dns: string;
  userId?: string;
  latency?: number;
  eventsEmitted?: number;
  errorCount?: number;
  performanceScore?: number;
  status?: 'active' | 'completed' | 'error';
}

@Injectable()
export class LoggerService implements NestLoggerService {
  private logger: winston.Logger;
  private activeRequests: Map<string, RequestMetrics>;

  constructor() {
    this.activeRequests = new Map();

    // Dynamically configure transports based on environment
    const transports: winston.transport[] = [new winston.transports.Console()];

    if (process.env.NODE_ENV === 'production') {
      // Use Google Cloud Logging in production
      try {
        const loggingWinston = new LoggingWinston({
          projectId: 'waw-backend-stage',
          logName: 'logs-metrics',
        });
        transports.push(loggingWinston);
      } catch (error) {
        console.error('Failed to initialize Google Cloud Logging:', error.message);
      }
    }

    // Initialize the logger with configured transports
    this.logger = winston.createLogger({
      level: 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
      ),
      transports,
    });
  }

  private generateRequestId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private calculatePerformanceScore(metrics: RequestMetrics): number {
    if (!metrics.latency) return 0;

    // Base score starts at 5
    let score = 5;

    // Deduct points based on latency
    if (metrics.latency > 5000) score -= 2; // > 5s
    else if (metrics.latency > 2000) score -= 1; // > 2s

    // Deduct for errors
    if (metrics.errorCount && metrics.errorCount > 0) {
      score -= Math.min(3, metrics.errorCount);
    }

    // Adjust for event throughput if streaming
    if (metrics.eventsEmitted) {
      const eventsPerSecond = (metrics.eventsEmitted / (metrics.latency / 1000));
      if (eventsPerSecond < 1) score -= 1;
    }

    return Math.max(1, Math.min(5, score));
  }

  initializeRequest(
    method: string,
    userAgent: string,
    ipAddress: string,
    dns: string,
    userId?: string
  ): string {
    const requestId = this.generateRequestId();
    const metrics: RequestMetrics = {
      requestId,
      method,
      startTime: Date.now(),
      userAgent,
      ipAddress,
      dns,
      userId,
      eventsEmitted: 0,
      errorCount: 0,
      status: 'active',
    };

    this.activeRequests.set(requestId, metrics);

    this.logger.info('gRPC Request Started', {
      ...metrics,
      timestamp: new Date().toISOString(),
      type: 'REQUEST_START',
    });

    return requestId;
  }

  logStreamEvent(requestId: string, eventType: string, details: any = {}): void {
    const metrics = this.activeRequests.get(requestId);
    if (!metrics) return;

    metrics.eventsEmitted = (metrics.eventsEmitted || 0) + 1;

    this.logger.info('Stream Event', {
      requestId,
      eventType,
      eventNumber: metrics.eventsEmitted,
      elapsedTime: Date.now() - metrics.startTime,
      ...details,
      type: 'STREAM_EVENT',
    });
  }

  logError(requestId: string, error: any): void {
    const metrics = this.activeRequests.get(requestId);
    if (!metrics) return;

    metrics.errorCount = (metrics.errorCount || 0) + 1;
    metrics.status = 'error';

    this.logger.error('Stream Error', {
      requestId,
      error: error.message,
      stack: error.stack,
      metrics,
      type: 'STREAM_ERROR',
    });
  }

  finalizeRequest(requestId: string): void {
    const metrics = this.activeRequests.get(requestId);
    if (!metrics) return;

    metrics.latency = Date.now() - metrics.startTime;
    metrics.status = 'completed';
    metrics.performanceScore = this.calculatePerformanceScore(metrics);

    this.logger.info('gRPC Request Completed', {
      ...metrics,
      timestamp: new Date().toISOString(),
      type: 'REQUEST_END',
      summary: {
        duration: `${metrics.latency}ms`,
        eventsEmitted: metrics.eventsEmitted,
        errorCount: metrics.errorCount,
        performanceScore: `${metrics.performanceScore}/5`,
      },
    });

    this.activeRequests.delete(requestId);
  }

  // Implement required NestLogger methods
  log(message: string, context?: any): void {
    this.logger.info(message, context);
  }

  error(message: string, trace?: string, context?: any): void {
    this.logger.error(message, { trace, ...context });
  }

  warn(message: string, context?: any): void {
    this.logger.warn(message, context);
  }

  debug(message: string, context?: any): void {
    this.logger.debug(message, context);
  }

  verbose(message: string, context?: any): void {
    this.logger.verbose(message, context);
  }
}