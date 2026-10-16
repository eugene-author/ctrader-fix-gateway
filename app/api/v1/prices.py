"""GET /prices, GET /prices/{symbol}."""

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from app.api.auth import get_authenticated_account_id
from app.api.dependencies import get_price_query
from app.core.metrics import price_stale_reads_total
from app.domain.errors import PriceUnavailableError
from app.schemas.prices import PriceResource, PricesResponse
from app.services.queries.get_price import GetPriceQuery, PriceView

router = APIRouter()


def _resource(view: PriceView) -> PriceResource:
    if view.stale:
        price_stale_reads_total.labels(symbol=view.symbol).inc()
    return PriceResource(
        symbol=view.symbol,
        bid=view.bid,
        ask=view.ask,
        spread=view.spread,
        timestamp=view.timestamp,
        age_ms=view.age_ms,
        stale=view.stale,
        source=view.source,
    )


@router.get("/prices/{symbol}", response_model=PriceResource)
async def get_price(
    symbol: str,
    account_id: UUID = Depends(get_authenticated_account_id),
    query: GetPriceQuery = Depends(get_price_query),
) -> PriceResource:
    """Return the latest cached quote for `symbol`, or 503 PRICE_UNAVAILABLE."""
    view = await query.handle(account_id, symbol)
    return _resource(view)


@router.get("/prices", response_model=PricesResponse)
async def get_prices(
    symbols: str = Query(..., description="Comma-separated symbol names, e.g. EURUSD,US500"),
    account_id: UUID = Depends(get_authenticated_account_id),
    query: GetPriceQuery = Depends(get_price_query),
) -> PricesResponse:
    """Return every requested symbol that has a cached quote; misses are omitted, not failed."""
    prices = []
    for symbol in (s.strip() for s in symbols.split(",")):
        if not symbol:
            continue
        try:
            prices.append(_resource(await query.handle(account_id, symbol)))
        except PriceUnavailableError:
            continue
    return PricesResponse(prices=prices)
