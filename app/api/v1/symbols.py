"""GET /symbols."""

from uuid import UUID

from fastapi import APIRouter, Depends

from app.api.auth import get_authenticated_account_id
from app.api.dependencies import get_symbols_query
from app.schemas.symbols import SymbolResource, SymbolsResponse
from app.services.queries.list_symbols import ListSymbolsQuery

router = APIRouter()


@router.get("/symbols", response_model=SymbolsResponse)
async def get_symbols(
    account_id: UUID = Depends(get_authenticated_account_id),
    query: ListSymbolsQuery = Depends(get_symbols_query),
) -> SymbolsResponse:
    """List every known symbol for this account, discovered or not."""
    views = await query.handle(account_id)
    return SymbolsResponse(
        symbols=[
            SymbolResource(
                symbol=view.symbol,
                enabled=view.enabled,
                subscribe=view.subscribe,
                needs_review=view.needs_review,
                broker_symbol_id=view.broker_symbol_id,
                lot_size=view.lot_size,
                qty_step=view.qty_step,
                min_qty=view.min_qty,
                max_qty=view.max_qty,
                price_step=view.price_step,
            )
            for view in views
        ]
    )
