import { Controller, Get } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { HealthService } from './health.service';
import { Observable } from 'rxjs';

// Define the interfaces from the health.proto file
interface HealthCheckRequest {
  service: string;
}

interface HealthCheckResponse {
  status: number; // Using number for the enum value
}

namespace HealthCheckResponse {
  export enum ServingStatus {
    UNKNOWN = 0,
    SERVING = 1,
    NOT_SERVING = 2,
    SERVICE_UNKNOWN = 3,
  }
}

@Controller('health')
export class HealthController {
  constructor(private readonly healthService: HealthService) {}

  @GrpcMethod('Health', 'Check')
  check(request: HealthCheckRequest): Promise<HealthCheckResponse> {
    return this.healthService.check(request);
  }

  @GrpcMethod('Health', 'Watch')
  watch(request: HealthCheckRequest): Observable<HealthCheckResponse> {
    return this.healthService.watch(request);
  }

  // Optional: HTTP endpoint for health checks
  @Get()
  async httpHealth(): Promise<{ status: string }> {
    const result = await this.healthService.check({ service: '' });
    // Convert numeric status to string representation
    const statusName = [
      'UNKNOWN',
      'SERVING',
      'NOT_SERVING',
      'SERVICE_UNKNOWN'
    ][result.status] || 'UNKNOWN';
    
    return { status: statusName };
  }
}
