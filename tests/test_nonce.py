import asyncio

from src.infra.nonce import NonceCoordinator


def test_shared_lock_for_account():
    async def inner():
        coord = NonceCoordinator()
        lock_a1 = await coord.get_lock("acct1")
        lock_a2 = await coord.get_lock("acct1")
        lock_b = await coord.get_lock("acct2")
        assert lock_a1 is lock_a2
        assert lock_a1 is not lock_b

    asyncio.run(inner())


def test_lock_serialization():
    async def inner():
        coord = NonceCoordinator()
        lock = await coord.get_lock("acctX")
        order = []

        async def task(name: str):
            async with lock:
                order.append(name)
                await asyncio.sleep(0.01)

        await asyncio.gather(*(task(str(i)) for i in range(3)))
        assert len(order) == 3

    asyncio.run(inner())
