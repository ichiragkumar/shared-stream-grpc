# gRPC Health Check Service

This module implements the standard gRPC health check protocol according to the [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).

## How it works

The health check service provides two RPC methods:

1. `Check` - Returns the current health status of the service
2. `Watch` - Streams health status updates whenever the status changes

The service checks:
- Server availability
- MongoDB database connectivity

## Using the Health Check

### From a gRPC client

You can use any gRPC client to check the health of your service. Here's an example using the `grpc-health-probe` tool:

```bash
# Install grpc-health-probe
# For Mac:
brew install grpc-health-probe

# For Linux:
curl -L https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.11/grpc_health_probe-linux-amd64 -o /usr/local/bin/grpc_health_probe
chmod +x /usr/local/bin/grpc_health_probe

# Check the overall service health
grpc_health_probe -addr=localhost:5000

# Check a specific service
grpc_health_probe -addr=localhost:5000 -service=coupon.CouponStreamService
```

### From JavaScript/TypeScript

```typescript
import { HealthClient } from 'grpc-health-check';
import { credentials } from '@grpc/grpc-js';

const healthClient = new HealthClient('localhost:5000', credentials.createInsecure());

// Check health
healthClient.check({ service: 'coupon.CouponStreamService' }, (err, response) => {
  if (err) {
    console.error('Health check failed:', err);
    return;
  }
  console.log('Health status:', response.status);
});

// Watch health changes
const watch = healthClient.watch({ service: 'coupon.CouponStreamService' });
watch.on('data', (response) => {
  console.log('Health status changed:', response.status);
});
watch.on('error', (err) => {
  console.error('Health watch error:', err);
});
```

### From Kubernetes

You can use the health check for Kubernetes liveness and readiness probes:

```yaml
livenessProbe:
  exec:
    command:
    - /bin/grpc_health_probe
    - -addr=:5000
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  exec:
    command:
    - /bin/grpc_health_probe
    - -addr=:5000
  initialDelaySeconds: 5
  periodSeconds: 5
```

## HTTP Health Check

For convenience, there is also an HTTP health check endpoint available at:

```
GET /health
```

This returns a simple JSON response:

```json
{
  "status": "SERVING"
}
```

Possible status values:
- `SERVING` - Service is healthy and ready to handle requests
- `NOT_SERVING` - Service is unhealthy and not ready to handle requests
- `UNKNOWN` - Service health status is unknown
- `SERVICE_UNKNOWN` - The requested service is not known to the health service
