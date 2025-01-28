import { Controller, Get, Param, Query, Post, Body, BadRequestException } from '@nestjs/common';
import { BusinessService } from './business.service';

@Controller('api/v1/business')
export class BusinessController {
  constructor(private readonly businessService: BusinessService) {}

  @Get('/get-businesses')
  async getBusinessesWithIds(
  @Query('ids') ids: string,
  @Query('languageCode') languageCode: string = 'en',
  @Query('brightness') brightness: string = 'light',
  ) {
  const idsArray = ids ? ids.split(',') : [];
  if (idsArray.length === 0) {
    throw new BadRequestException('Invalid request: ids are missing.');
  }

  return this.businessService.getBusinessesWithIds(idsArray, languageCode, brightness);
  } 


  @Get("business-branches")
  async getBusinessBranchDetails(
    @Query('businessId') businessId: string,
    @Query('businessBranchId') businessBranchId: string,
    @Query('languageCode') languageCode: string = 'en',
  ) {
    if (!businessId || businessId.length === 0) {
      throw new BadRequestException('Invalid request: businessId is missing.');
    }

    if (!businessBranchId || businessBranchId.length === 0) {
      throw new BadRequestException('Invalid request: businessBranchId is missing.');
    }

    return this.businessService.getBusinessBranchDetails(businessId, businessBranchId, languageCode);
  }


  @Get('/:id')
  async getBusinessById(
    @Param('id') id: string,
    @Query('languageCode') languageCode: string = 'en',
    @Query('brightness') brightness: string = 'light',
  ) {

    if(!id || id.length === 0) {
      throw new BadRequestException('Invalid request: id is missing.');
    }
    return this.businessService.getBusinessById(id, languageCode, brightness);
  }


  
}
