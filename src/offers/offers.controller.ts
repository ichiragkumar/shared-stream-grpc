import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { OffersService } from './offers.service';

@Controller('api/v1/offers')
export class OffersController {
    constructor(private readonly offersService: OffersService) {}


    @Get('/spefcial-offers')
    async getSpecialOffers(
        @Query('ids') ids: string,
        @Query('languageCode') languageCode: string = 'en',
        @Query('brightness') brightness: string = 'light',
    ) {
        const idsArray = ids ? ids.split(',') : [];
        if (idsArray.length === 0) {
            throw new BadRequestException('Invalid request: ids are missing. or atleast one id is required.');
        }
        return this.offersService.getSpecialOffers(idsArray, languageCode, brightness);
    }
}
