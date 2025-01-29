import { Controller } from '@nestjs/common';
import { GrpcMethod, GrpcStreamMethod } from '@nestjs/microservices';
import { Observable } from 'rxjs';
import { CouponService } from './coupon.service';
import { 
  StatusFilter, 
  CouponIssue, 
  UserFilter,
  MoreCouponRequest,
  CouponIssueWithBusiness,
  ActiveBusinessesStreamResponse,
  ActiveCouponStreamResponse,
  Balance
} from "../generated/coupon_stream";

@Controller()
export class CouponGrpcController {
  constructor(private readonly couponService: CouponService) {}

  @GrpcMethod('CouponStreamService', 'StreamCouponIssues')
  StreamCouponIssues(data: StatusFilter): Observable<CouponIssue> {
    return this.couponService.streamCouponIssuesService(data);
  }

  @GrpcMethod('CouponStreamService', 'StreamMoreCouponRequests')
  StreamMoreCouponRequests(data: UserFilter): Observable<MoreCouponRequest> {
    return this.couponService.streamMoreCouponRequestsService(data);
  }


  @GrpcMethod('CouponStreamService', 'ActiveCouponIssuesWithBusinessesStream')
  ActiveCouponIssuesWithBusinessesStream(): Observable<CouponIssueWithBusiness> {
  return this.couponService.streamActiveCouponIssuesWithBusinessService();
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveBusinessesStream')
  streamActiveBusinessesStream(): Observable<ActiveBusinessesStreamResponse> {
    return this.couponService.streamActiveBusinessesWithContractTypesService();
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveCoupons')
  streamActiveCouponsStream(data: StatusFilter): Observable<ActiveCouponStreamResponse> {
    return this.couponService.streamActiveCouponsStreamService(data);
  }

  @GrpcMethod('CouponStreamService', 'WalletStream')
  streamWalletController(data: UserFilter): Observable<Balance> {
    return this.couponService.streamWalletService(data);
  }
}