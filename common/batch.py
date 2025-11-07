from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Callable, Iterator, List

from tqdm import tqdm


def batch_processor(
    items: List[Any],
    worker_func: Callable[..., Any],
    max_workers: int = 8,
    chunk_size: int = 500,
) -> Iterator[List[Any]]:
    """
    一个通用的批量并发处理器，它按批次返回结果。

    Args:
        items (List[Any]): 需要处理的项目列表。
        worker_func (Callable[..., Any]): 应用于每个项目的工作函数。
        ...

    Yields:
        Iterator[List[Any]]: 为每个处理完成的批次，返回一个包含所有成功结果的列表。
    """
    item_chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

    print(
        f"开始处理 {len(items)} 个项目, 共 {len(item_chunks)} 批, 每批最多 {chunk_size} 个。"
    )

    for i, chunk in enumerate(item_chunks):
        print(f"\n--- 正在处理第 {i + 1}/{len(item_chunks)} 批 ---")

        chunk_results = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {
                executor.submit(worker_func, item): item for item in chunk
            }
            progress_bar = tqdm(
                as_completed(future_to_item),
                total=len(chunk),
            )
            for future in progress_bar:
                item = future_to_item[future]
                try:
                    result = future.result()
                    if result is not None:
                        chunk_results.append(result)
                except Exception as e:
                    print(f"❌ 项目 {item} 处理失败: {e}")

        if chunk_results:
            yield chunk_results
