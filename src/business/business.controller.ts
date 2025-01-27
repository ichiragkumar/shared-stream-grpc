import { Controller, Get, Param, Query, Post, Body } from '@nestjs/common';
import { BusinessService } from './business.service';

@Controller('api/v1/business')
export class BusinessController {
  constructor(private readonly businessService: BusinessService) {}

  @Get('/get-businesses')
  async getBusinessesWithIds(
  @Body() requestData: {
    ids: string[];
    languageCode?: string;
    brightness?: string;
    },
    ) {
    const languageCode = requestData.languageCode || 'en';
    const brightness = requestData.brightness || 'light';

    return this.businessService.getBusinessesWithIds(requestData.ids, languageCode, brightness);
  }

  @Get('/:id')
  async getBusinessById(
    @Param('id') id: string,
    @Query('languageCode') languageCode: string = 'en',
    @Query('brightness') brightness: string = 'light',
  ) {
    return this.businessService.getBusinessById(id, languageCode, brightness);
  }


  
}
