import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { OffersService } from './offers.service';
import { Brightness, Language } from 'src/config/constant';

@Controller('api/v1/offers')
export class OffersController {
    constructor(private readonly offersService: OffersService) {}


    @Get('/spefcial-offers')
    async getSpecialOffers(
        @Query('ids') ids: string,
        @Query('languageCode') languageCode: string = Language.DEFAULT,
        @Query('brightness') brightness: string = Brightness.DEFAULT,
    ) {
        const idsArray = ids ? ids.split(',') : [];
        if (idsArray.length === 0) {
            throw new BadRequestException('Invalid request: ids are missing. or atleast one id is required.');
        }
        return this.offersService.getSpecialOffers(idsArray, languageCode, brightness);
    }
}
