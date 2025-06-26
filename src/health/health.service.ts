import { Injectable } from '@nestjs/common';
import { status } from '@grpc/grpc-js';
import { DatabaseService } from '../config/database.config';
import { Observable, Subject } from 'rxjs';

// Define the interfaces from the health.proto file
interface HealthCheckRequest {
  service: string;
}

interface HealthCheckResponse {
  status: HealthCheckResponse.ServingStatus;
}

namespace HealthCheckResponse {
  export enum ServingStatus {
    UNKNOWN = 0,
    SERVING = 1,
    NOT_SERVING = 2,
    SERVICE_UNKNOWN = 3,
  }
}

@Injectable()
export class HealthService {
  private readonly statusMap: Map<string, HealthCheckResponse.ServingStatus> = new Map();
  private readonly statusChangeSubjects: Map<string, Subject<HealthCheckResponse>> = new Map();

  constructor() {
    // Default status is SERVING
    this.statusMap.set('', HealthCheckResponse.ServingStatus.SERVING); 
    this.statusMap.set('grpc.health.v1.Health', HealthCheckResponse.ServingStatus.SERVING);
    this.statusMap.set('coupon.CouponStreamService', HealthCheckResponse.ServingStatus.SERVING);
  }

  /**
   * Check the health status of a service
   */
  async check(request: HealthCheckRequest): Promise<HealthCheckResponse> {
    const service = request.service;
    
    // Check database connectivity
    try {
      const client = DatabaseService.getClient();
      if (!client) {
        this.setStatus(service, HealthCheckResponse.ServingStatus.NOT_SERVING);
        return {
          status: HealthCheckResponse.ServingStatus.NOT_SERVING,
        };
      }
      
      // Perform a ping to check if the database is responsive
      // Wrap in try/catch in case the db() or admin() methods aren't available
      try {
        await client.db().admin().ping();
      } catch (innerErr) {
        console.error('Database ping error:', innerErr);
        throw innerErr; // Re-throw to be caught by outer catch
      }
    } catch (error) {
      console.error('Health check database error:', error);
      this.setStatus(service, HealthCheckResponse.ServingStatus.NOT_SERVING);
      return {
        status: HealthCheckResponse.ServingStatus.NOT_SERVING,
      };
    }

    // If service is specified but not found in our map
    if (service && !this.statusMap.has(service)) {
      return {
        status: HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN,
      };
    }

    // Return the status from our map, or default to UNKNOWN
    return {
      status: this.statusMap.get(service) || HealthCheckResponse.ServingStatus.UNKNOWN,
    };
  }

  /**
   * Set the health status of a service
   */
  setStatus(service: string, status: HealthCheckResponse.ServingStatus): void {
    this.statusMap.set(service, status);
    
    // Notify any watchers about the status change
    if (this.statusChangeSubjects.has(service)) {
      const subject = this.statusChangeSubjects.get(service);
      if (subject) {
        subject.next({ status });
      }
    }
  }

  /**
   * Watch for health status changes for a service
   */
  watch(request: HealthCheckRequest): Observable<HealthCheckResponse> {
    const service = request.service;
    
    // Create a subject for this service if it doesn't exist
    if (!this.statusChangeSubjects.has(service)) {
      this.statusChangeSubjects.set(service, new Subject<HealthCheckResponse>());
    }
    
    // Get the subject we just created/found
    const subject = this.statusChangeSubjects.get(service);
    if (subject) {
      setTimeout(() => {
        const status = this.statusMap.get(service) || HealthCheckResponse.ServingStatus.UNKNOWN;
        subject.next({ status });
      }, 0);
      
      // Return the observable for subscribers
      return subject.asObservable();
    }
    
    // Create a new subject if one wasn't found (shouldn't happen)
    const newSubject = new Subject<HealthCheckResponse>();
    this.statusChangeSubjects.set(service, newSubject);
    return newSubject.asObservable();
  }

  /**
   * Get health service status mapping
   */
  getStatusMap(): Map<string, HealthCheckResponse.ServingStatus> {
    return this.statusMap;
  }
}
