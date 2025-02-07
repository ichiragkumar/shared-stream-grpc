import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { 
  CouponIssue, 
  MoreCouponRequest,
  CouponIssueWithBusiness,
  ActiveBusinessesStreamResponse,
  ActiveCouponStreamResponse,
  UserPrefrences,
  WalletBalanceResponse,
  ActiveDrawnResponse,
  User
} from "../generated/coupon_stream"
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { streamCouponIssues } from './streams/streamCouponIssue';
import { streamActiveCouponIssuesWithBusiness } from './streams/streamActiveCouponIssuesWithBusiness';
import { streamActiveBusinessesWithContractTypes } from './streams/streamActiveBusinessesWithContractTypes';
import {streamWalletBalance} from "./streams/streamWalletBalance"
import { streamActiveDrawn } from './streams/streamActiveDrawn';
import { streamMoreCouponRequestsService } from './streams/streamMoreCouponRequests';
import { streamActiveCouponsStream } from './streams/streamActiveCouponsStream';
import { LoggerService } from '../logger/logger.service';



interface ActiveCouponIssueWithBusiness {
    id: string;
    status: string;
    businessId: string;
    updatedAt: number;
    businessName?: string;
    businessLogo?: string;
    couponIssueId: string;
    couponName: string;
}

  

@Injectable()
export class CouponService {
  private db: Db;
  constructor(private readonly logger: LoggerService) {}

  async onModuleInit() {
    this.db = await DatabaseService.connect();
    this.logger.log('Database connected successfully.');
  }

  streamCouponIssuesService(data: UserPrefrences ): Observable<CouponIssue> {
    this.logger.log('streamCouponIssuesService called', { userPrefrences: data });
    return streamCouponIssues(data, this.db, this.logger);
  }

  streamMoreCouponRequestsService(data: User): Observable<MoreCouponRequest> {
    this.logger.log('streamMoreCouponRequestsService called', { user: data });
    return streamMoreCouponRequestsService( this.db, data, this.logger);
  }

  

  streamActiveCouponIssuesWithBusinessService(data: UserPrefrences): Observable<CouponIssueWithBusiness> {
    this.logger.log('streamActiveCouponIssuesWithBusinessService called', { userPrefrences: data });
    return streamActiveCouponIssuesWithBusiness(this.db, data, this.logger);
}
 

  streamActiveBusinessesWithContractTypesService(data: UserPrefrences): Observable<ActiveBusinessesStreamResponse> {
    this.logger.log('streamActiveBusinessesWithContractTypesService called', { userPrefrences: data });
    return streamActiveBusinessesWithContractTypes(data, this.db, this.logger
    );
  }

  streamActiveCouponsStreamService(data: User): Observable<ActiveCouponStreamResponse> {
    this.logger.log('streamActiveCouponsStreamService called', { user: data });
    return streamActiveCouponsStream(this.db, data, this.logger);
  }

  streamWalletService(data: User): Observable<WalletBalanceResponse > {
    this.logger.log('streamWalletService called', { user: data });
    return streamWalletBalance(this.db, data, this.logger);
  }

  streamActiveDrawnService(data: UserPrefrences): Observable<ActiveDrawnResponse>{
    this.logger.log('streamActiveDrawnService called', { userPrefrences: data });
    return streamActiveDrawn(this.db, data, this.logger);

  }

  

}