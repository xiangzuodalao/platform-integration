from sqlalchemy.ext.asyncio import AsyncSession

from platform_integration.models.bindings import (
    EquipmentMapping,
    MeasurementBinding,
    TenantBinding,
)


class BindingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_tenant(self, binding: TenantBinding) -> TenantBinding:
        self._session.add(binding)
        await self._session.flush()
        return binding

    async def add_equipment(self, mapping: EquipmentMapping) -> EquipmentMapping:
        self._session.add(mapping)
        await self._session.flush()
        return mapping

    async def add_measurement(self, binding: MeasurementBinding) -> MeasurementBinding:
        self._session.add(binding)
        await self._session.flush()
        return binding
