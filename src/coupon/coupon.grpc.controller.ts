import { Controller } from '@nestjs/common';
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
  Balance,
  UserPrefrences,
  WalletBalanceResponse,
  ActiveDrawnResponse,
  User,
  TicketStreamResponse,
  ZoneStreamResponse,
  BusinessBranchStreamResponse,
  UserCartStreamResponse,
  UserCartStreamItem,
  UserNotificationStreamResponse,
  MainUser,
  EmptyRequest,
  EnvironmentResponse,
} from '../generated/coupon_stream';
import { LoggerService } from '../logger/logger.service';
import { Metadata } from '@grpc/grpc-js';
import { GrpcMethod } from '@nestjs/microservices';
import { GrpcStreamLoggerHelper } from 'src/logger/grpc-stream-logger.helper';

@Controller()
export class CouponGrpcController {
  constructor(
    private readonly couponService: CouponService,
    private readonly logger: LoggerService,
  ) {}

  @GrpcMethod('CouponStreamService', 'StreamCouponIssues')
  StreamCouponIssues(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<CouponIssue> {
    const stream$ = this.couponService.streamCouponIssuesService(data);
    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamCouponIssues',
      metadata,
      this.logger,
      stream$,
      (couponIssue) => ({
        issueId: couponIssue.id,
        businessId: couponIssue.businessId,
        status: couponIssue.status,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'ActiveCouponIssuesWithBusinessesStream')
  ActiveCouponIssuesWithBusinessesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<CouponIssueWithBusiness> {
    const stream$ =
      this.couponService.streamActiveCouponIssuesWithBusinessService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'ActiveCouponIssuesWithBusinessesStream',
      metadata,
      this.logger,
      stream$,
      (couponIssue) => ({
        issueId: couponIssue.couponIssueId,
        businessId: couponIssue.businessId,
        status: couponIssue.status,
        couponName: couponIssue.couponName,
        businessName: couponIssue.businessName,
        amountLeft: couponIssue.amountLeft,
        currency: couponIssue.currency,
        type: couponIssue.type,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveBusinessesStream')
  streamActiveBusinessesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ActiveBusinessesStreamResponse> {
    const stream$ =
      this.couponService.streamActiveBusinessesWithContractTypesService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamActiveBusinessesStream',
      metadata,
      this.logger,
      stream$,
      (business) => ({
        businessId: business.businessId,
        contractType: business.contractType,
        title: business.title,
        suspended: business.suspended,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveCoupons')
  streamActiveCouponsStream(
    data: { userId: string },
    metadata: Metadata,
  ): Observable<ActiveCouponStreamResponse> {
    const stream$ = this.couponService.streamActiveCouponsStreamService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamActiveCoupons',
      metadata,
      this.logger,
      stream$,
      (couponResponse) => ({
        businessId: couponResponse.businessId,
        code: couponResponse.code,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamMoreCouponRequests')
  StreamMoreCouponRequests(
    data: User,
    metadata: Metadata,
  ): Observable<MoreCouponRequest> {
    const stream$ = this.couponService.streamMoreCouponRequestsService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamMoreCouponRequests',
      metadata,
      this.logger,
      stream$,
      (moreCouponRequest) => ({
        requestId: moreCouponRequest.id,
        userId: moreCouponRequest.userId,
        couponIssueId: moreCouponRequest.couponIssueId,
        createdAt: moreCouponRequest.createdAt,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'WalletStream')
  streamWalletController(
    data: User,
    metadata: Metadata,
  ): Observable<WalletBalanceResponse> {
    const stream$ = this.couponService.streamWalletService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'WalletStream',
      metadata,
      this.logger,
      stream$,
      (walletBalance) => ({
        availableUSD: walletBalance.availableBalances?.USD,
        availableEGP: walletBalance.availableBalances?.EGP,
        blockedUSD: walletBalance.blockedBalances?.USD,
        blockedEGP: walletBalance.blockedBalances?.EGP,
        streamType: walletBalance.streamType,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamActiveDrawn')
  StreamActiveDrawn(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ActiveDrawnResponse> {
    const stream$ = this.couponService.streamActiveDrawnService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamActiveDrawn',
      metadata,
      this.logger,
      stream$,
      (activeDrawn) => ({
        drawnId: activeDrawn.id,
        businessId: activeDrawn.businessId,
        status: activeDrawn.status,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'TicketsStream')
  TicketsStream(
    data: User,
    metadata: Metadata,
  ): Observable<TicketStreamResponse> {
    const stream$ = this.couponService.TicketsStreamService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'TicketsStream',
      metadata,
      this.logger,
      stream$,
      (ticket) => ({
        id: ticket.id,
        userId: ticket.userId,
        drawId: ticket.drawId,
        drawType: ticket.drawType,
        isDrawClosed: ticket.isDrawClosed,
        drawNumbers: ticket.drawNumbers,
        createdAt: ticket.createdAt,
        status: ticket.status,
        streamType: ticket.streamType,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'ZonesStream')
  ZonesStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<ZoneStreamResponse> {
    const stream$ = this.couponService.ZonesStreamService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'ZonesStream',
      metadata,
      this.logger,
      stream$,
      (zone) => ({
        id: zone.id,
        country: zone.country,
        createdAt: zone.createdAt,
        isDefault: zone.isDefault,
        name: zone.name,
        location: zone.location,
        streamType: zone.streamType,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'BusinessBranchStream')
  BusinessBranchStream(
    data: UserPrefrences,
    metadata: Metadata,
  ): Observable<BusinessBranchStreamResponse> {
    const stream$ = this.couponService.BusinessBranchStreamService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'BusinessBranchStream',
      metadata,
      this.logger,
      stream$,
      (branch) => ({
        id: branch.id,
        businessSuspended: branch.businessSuspended,
        shortAddress: branch.shortAddress,
        businessId: branch.businessId,
        zoneId: branch.zoneId,
        location: branch.location,
        openingHours: branch.openingHours,
        createdAt: branch.createdAt,
        contractTypes: branch.contractTypes,
        streamType: branch.streamType,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamUserCarts')
  StreamUserCarts(
    data: User,
    metadata: Metadata,
  ): Observable<UserCartStreamResponse> {
    const stream$ = this.couponService.UserCartStreamResponseService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamUserCarts',
      metadata,
      this.logger,
      stream$,
      (response: UserCartStreamResponse) => {
        if (response?.items?.length) {
          const filteredItems = response.items.filter(
            (item) => item.itemId && item.amount,
          );

          if (filteredItems.length) {
            const streamType = filteredItems[0].streamType;

            return {
              totalItems: filteredItems.length,
              streamType,
              items: filteredItems.map((item) => ({
                itemId: item.itemId,
                amount: item.amount,
                purchasePrice: item.purchasePrice,
                currency: item.currency,
                feePrice: item.feePrice || 0,
                taxAmount: item.taxAmount || 0,
              })),
            };
          }
        }
        return {};
      },
    );
  }

  @GrpcMethod('CouponStreamService', 'StreamUserNotifications')
  StreamUserNotifications(
    data: MainUser,
    metadata: Metadata,
  ): Observable<UserNotificationStreamResponse> {
    const stream$ = this.couponService.streamUserNotificationsService(data);

    return GrpcStreamLoggerHelper.createStreamHandler(
      'StreamUserNotifications',
      metadata,
      this.logger,
      stream$,
      (userNotificationStreamResponse) => ({
        id: userNotificationStreamResponse.id,
        isRead: userNotificationStreamResponse.isRead,
        createdAt: userNotificationStreamResponse.createdAt,
        title: userNotificationStreamResponse.title,
        body: userNotificationStreamResponse.body,
        topic: userNotificationStreamResponse.topic,
        screen: userNotificationStreamResponse.screen,
        userId: userNotificationStreamResponse.userId,
        streamType: userNotificationStreamResponse.streamType,
      }),
    );
  }

  @GrpcMethod('CouponStreamService', 'EnvironmentStream')
  environmentStream(
    request: EmptyRequest,
    metadata: Metadata,
  ): Observable<EnvironmentResponse> {
    const stream$ = this.couponService.environmentStreamService();

    return GrpcStreamLoggerHelper.createStreamHandler(
      'EnvironmentStream',
      metadata,
      this.logger,
      stream$,
      (env) => ({
        id: env.id,
        allowInvites: env.allowInvites,
        allowInviteAll: env.allowInviteAll,
        stage: env.stage,
        deleteUnsentReports: env.deleteUnsentReports,
        useCrashlytics: env.useCrashlytics,
        auditLogsCredentials: env.auditLogsCredentials,
        requiredMinimumAndroidVersion: env.requiredMinimumAndroidVersion,
        requiredMiliumOSVersion: env.requiredMinimumiOSVersion,
        deleteAndroidUnsentReports: env.deleteAndroidUnsentReports,
        deleteiOSUnsentReports: env.deleteiOSUnsentReports,
        useAndroidCrashlytics: env.useAndroidCrashlytics,
        useiOSCrashlytics: env.useiOSCrashlytics,
        streamType: env.streamType,
      }),
    );
  }
}
