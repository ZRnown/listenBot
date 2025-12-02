import asyncio
import random
from typing import List

from core.clients import ClientManager
from core.filters import normalize_text_for_matching
from services import settings_service
from storage import dao_accounts


async def parse_and_execute_click(manager: ClientManager, link_text: str, report_chat_id: int):
    """解析链接并执行点击任务（自动识别链接）。

    从原 control_bot.py 中抽离，仅保留点击相关逻辑。
    """
    try:
        # 解析消息链接
        chat_id_from_link = None
        msg_id_from_link = None

        print(f"[自动点击] 开始解析链接: {link_text}")

        # 解析 t.me/c/xxx/123 格式（超级群组/频道）
        import re
        try:
            match1 = re.search(r't\.me/c/(\d+)/(\d+)', link_text)
            if match1:
                channel_id = match1.group(1)
                msg_id_from_link = int(match1.group(2))
                chat_id_from_link = int(f'-100{channel_id}')
                print(f"[自动点击] 解析成功（频道格式）: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}")
            else:
                # 解析 t.me/username/123 格式
                match2 = re.search(r't\.me/([a-zA-Z0-9_]+)/(\d+)', link_text)
                if match2:
                    username = match2.group(1)
                    msg_id_from_link = int(match2.group(2))
                    chat_id_from_link = username
                    print(f"[自动点击] 解析成功（用户名格式）: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}")
        except Exception as parse_error:
            print(f"[自动点击] ❌ 解析链接时出错: {parse_error}")
            return False, f'解析链接时出错：{parse_error}'

        # 验证链接格式
        if not chat_id_from_link or not msg_id_from_link:
            print(f"[自动点击] ❌ 链接解析失败 - chat_id或msg_id为空")
            return False, '消息链接格式无效'

        # 获取所有点击账号
        try:
            from bot.control_bot import list_accounts  # 避免循环导入，仅在运行时引用
            click_accounts = list_accounts('click')
            print(f"[自动点击] 找到 {len(click_accounts)} 个点击账号")
        except Exception as list_error:
            print(f"[自动点击] ❌ 获取账号列表失败: {list_error}")
            return False, f'获取账号列表失败：{list_error}'

        if not click_accounts:
            print(f"[自动点击] ⚠️ 没有可用的点击账号")
            return False, '没有可用的点击账号'

        # 异步执行点击任务（不阻塞）
        print(f"[自动点击] 🚀 创建异步任务: Chat ID={chat_id_from_link}, Message ID={msg_id_from_link}, 账号数={len(click_accounts)}")

        async def safe_start_click_job():
            """安全包装的点击任务，确保所有异常都被捕获并反馈"""
            from bot.click_tasks import start_click_job  # 避免循环导入
            try:
                await start_click_job(
                    manager, chat_id_from_link, msg_id_from_link, click_accounts, report_chat_id
                )
            except Exception as e:
                print(f"[自动点击] ❌ 任务执行异常: {e}")
                import traceback
                traceback.print_exc()
                try:
                    error_msg = (
                        f'❌ **点击任务执行失败**\\n'
                        f'━━━━━━━━━━━━━━━━\\n'
                        f'错误信息：`{str(e)}`\\n\\n'
                        f'请检查：\\n'
                        f'• 账号是否在线\\n'
                        f'• 消息链接是否正确\\n'
                        f'• 账号是否已加入目标群组'
                    )
                    await manager.bot.send_message(
                        report_chat_id,
                        error_msg,
                        parse_mode='markdown',
                    )
                except Exception as send_error:
                    print(f"[自动点击] ❌ 发送错误消息失败: {send_error}")

        # 创建并立即调度任务
        asyncio.create_task(safe_start_click_job())
        return True, None

    except Exception as e:
        print(f"[自动点击] ❌ 处理过程中出现未捕获的异常: {e}")
        import traceback
        traceback.print_exc()
        return False, f'处理失败：{e}'


async def start_click_job(manager: ClientManager, target_chat_id, target_msg_id, accounts: List[dict], report_chat_id: int):
    """开始点击任务：获取消息、匹配关键词并并发点击（控制并发数避免封号）。

    逻辑整体从原 control_bot.py 迁移过来，保持行为不变。
    """
    bot = manager.bot
    if not bot:
        print(f"[点击任务] ❌ 控制机器人未初始化")
        return

    print(f"[点击任务] 🚀 开始点击任务: Chat ID={target_chat_id}, Message ID={target_msg_id}, 账号数={len(accounts)}")
    try:
        # 使用第一个可用的账号客户端获取消息
        target_msg = None
        buttons = None
        button_positions = []
        error_details = []  # 记录所有尝试的错误信息
        actual_chat_id = None  # 真实的 Chat ID（从消息对象中获取）

        print(f"[点击任务] 开始尝试获取消息，共有 {len(accounts)} 个账号")
        print(f"[点击任务] 当前在线账号数: {len(manager.account_clients)}")
        print(f"[点击任务] 在线账号ID列表: {list(manager.account_clients.keys())}")

        for acc in accounts:
            acc_id = acc['id']
            acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
            client = manager.account_clients.get(acc_id)
            if not client:
                print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 客户端不存在")
                error_details.append(f"账号 {acc_name} (#{acc_id}): 客户端不存在")
                continue

            # 检查客户端是否真正连接
            try:
                if not client.is_connected():
                    print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 客户端未连接")
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 客户端未连接")
                    continue
            except Exception as conn_check_error:
                print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 检查连接状态失败: {conn_check_error}")
                error_details.append(f"账号 {acc_name} (#{acc_id}): 连接状态检查失败")
                continue

            try:
                print(f"[点击任务] 尝试使用账号 {acc_name} (#{acc_id}) 获取消息...")
                target_msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                if target_msg:
                    # 从消息对象中获取真实的 Chat ID
                    try:
                        if hasattr(target_msg, 'chat_id'):
                            actual_chat_id = target_msg.chat_id
                        elif hasattr(target_msg, 'peer_id'):
                            peer = target_msg.peer_id
                            if hasattr(peer, 'channel_id'):
                                actual_chat_id = int(f'-100{peer.channel_id}')
                            elif hasattr(peer, 'chat_id'):
                                actual_chat_id = -peer.chat_id
                            elif hasattr(peer, 'user_id'):
                                actual_chat_id = peer.user_id
                        # 如果还是获取不到，尝试从消息的 chat 属性获取
                        if actual_chat_id is None:
                            try:
                                chat = await target_msg.get_chat()
                                if chat:
                                    actual_chat_id = chat.id
                            except:
                                pass
                    except Exception as chat_id_error:
                        print(f"[点击任务] ⚠️ 获取真实 Chat ID 失败: {chat_id_error}")
                    
                    buttons = getattr(target_msg, 'buttons', None)
                    if buttons:
                        for i, row in enumerate(buttons):
                            for j, btn in enumerate(row):
                                btn_text = getattr(btn, 'text', None) or ''
                                button_positions.append((i, j, btn_text))
                    print(f"[点击任务] ✅ 账号 {acc_name} (#{acc_id}) 成功获取消息，找到 {len(button_positions)} 个按钮，Chat ID={actual_chat_id}")
                    break
                else:
                    print(f"[点击任务] ⚠️ 账号 {acc_name} (#{acc_id}) 获取的消息为空")
            except Exception as e:
                error_str = str(e)
                # 判断错误类型
                if 'CHANNEL_PRIVATE' in error_str or 'CHAT_FORBIDDEN' in error_str or 'USER_BANNED_IN_CHANNEL' in error_str:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 未加入该群组/频道或已被禁止")
                elif 'MESSAGE_NOT_FOUND' in error_str or 'MSG_ID_INVALID' in error_str:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): 消息不存在或无效")
                else:
                    error_details.append(f"账号 {acc_name} (#{acc_id}): {error_str}")
                continue

        if not target_msg:
            error_msg = (
                f'❌ **无法获取消息**\\n'
                f'━━━━━━━━━━━━━━━━\\n'
                f'📋 消息链接：Chat ID: `{target_chat_id}`, Message ID: `{target_msg_id}`\\n\\n'
                f'**尝试了 {len(accounts)} 个账号，全部失败：**\\n'
            )
            if error_details:
                for i, detail in enumerate(error_details[:10], 1):  # 最多显示10个错误
                    error_msg += f'{i}. {detail}\\n'
                if len(error_details) > 10:
                    error_msg += f'... 还有 {len(error_details) - 10} 个账号失败\\n'
            else:
                error_msg += '（无可用账号客户端）\\n'

            error_msg += (
                f'\\n**可能的原因：**\\n'
                f'1. ⚠️ **所有账号都未加入该群组/频道**（最常见）\\n'
                f'2. 消息链接无效或消息已被删除\\n'
                f'3. 账号没有访问该消息的权限\\n'
                f'4. 账号已被群组/频道管理员禁止\\n\\n'
                f'💡 **解决方案：**\\n'
                f'• 确保至少有一个点击账号已加入目标群组/频道\\n'
                f'• 检查消息链接是否正确\\n'
                f'• 使用"🚪 自动进群"功能让账号加入群组'
            )
            try:
                await bot.send_message(report_chat_id, error_msg, parse_mode='markdown')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送消息失败: {send_error}")
            return

        if not buttons or not button_positions:
            try:
                await bot.send_message(report_chat_id, '⚠️ 该消息没有按钮')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送消息失败: {send_error}")
            return

        # 检查哪些账号有关键词匹配
        matched_accounts = []
        print(f"[点击任务] 开始匹配关键词，按钮数量: {len(button_positions)}")
        for acc in accounts:
            acc_id = acc['id']
            keywords = settings_service.get_account_keywords(acc_id, kind='click') or []
            print(f"[点击任务] 账号 #{acc_id} 的点击关键词: {keywords}")
            if not keywords:
                print(f"[点击任务] ⚠️ 账号 #{acc_id} 没有设置点击关键词")
                continue
            for i, j, btn_text in button_positions:
                matched_kw = None
                # 规范化按钮文本（去除emoji、零宽字符、空格）
                normalized_btn_text = normalize_text_for_matching(btn_text)
                print(f"[点击任务] 按钮文本: '{btn_text}' -> 规范化后: '{normalized_btn_text}'")
                for k in keywords:
                    if not k:
                        continue
                    # 规范化关键词（去除空格）
                    normalized_keyword = k.strip()
                    # 检查关键词是否在规范化后的按钮文本中
                    if normalized_keyword and normalized_keyword in normalized_btn_text:
                        matched_kw = k
                        break
                if matched_kw:
                    print(f"[点击任务] ✅ 账号 #{acc_id} 匹配到按钮 '{btn_text}' (关键词: {matched_kw})")
                    matched_accounts.append((acc, i, j, btn_text))
                    break

        if not matched_accounts:
            all_btn_texts = [bt[2] for bt in button_positions]
            print(f"[点击任务] ⚠️ 没有账号的关键词匹配到按钮")
            print(f"[点击任务] 按钮文本列表: {all_btn_texts}")
            print(f"[点击任务] 检查所有账号的关键词...")

            # 显示所有账号的关键词，帮助用户调试
            keywords_info = []
            no_keywords_accounts = []
            for acc in accounts:
                acc_id = acc['id']
                acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
                keywords = settings_service.get_account_keywords(acc_id, kind='click') or []
                if keywords:
                    keywords_info.append(f"账号 {acc_name}: {', '.join(keywords[:5])}")
                else:
                    no_keywords_accounts.append(acc_name)

            error_msg = (
                f'⚠️ **没有账号的关键词匹配到按钮**\\n'
                f'━━━━━━━━━━━━━━━━\\n'
                f'📋 按钮文本：{", ".join(all_btn_texts[:5])}{"..." if len(all_btn_texts) > 5 else ""}\\n\\n'
            )

            if no_keywords_accounts:
                error_msg += f'**未设置点击关键词的账号：**\\n'
                for acc_name in no_keywords_accounts:
                    error_msg += f'• {acc_name}\\n'
                error_msg += '\\n'

            if keywords_info:
                error_msg += f'**当前点击关键词：**\\n'
                for info in keywords_info[:10]:
                    error_msg += f'• {info}\\n'
                error_msg += '\\n'

            error_msg += (
                f'💡 **提示：**\\n'
                f'• 检查按钮文本是否包含您设置的关键词\\n'
                f'• 关键词匹配是大小写敏感的\\n'
                f'• 可以在账号设置中添加或修改点击关键词\\n'
                f'• 未设置关键词的账号不会参与点击'
            )

            try:
                await bot.send_message(report_chat_id, error_msg, parse_mode='markdown')
            except Exception as send_error:
                print(f"[点击任务] ❌ 发送消息失败: {send_error}")
            return

        # 不发送开始报告，只在最终报告中显示结果
        all_btn_texts = [bt[2] for bt in button_positions]
        print(f"[点击任务] 开始执行点击，匹配账号数：{len(matched_accounts)}，按钮文本：{', '.join(all_btn_texts[:3])}")

        # 优化：预先为所有账号获取消息对象，避免重复获取
        print(f"[点击任务] 🚀 开始预获取消息对象，共 {len(matched_accounts)} 个账号")
        account_messages = {}  # acc_id -> message object
        pre_fetch_semaphore = asyncio.Semaphore(20)  # 预获取并发数
        
        async def pre_fetch_message(acc):
            acc_id = acc['id']
            acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"
            client = manager.account_clients.get(acc_id)
            if not client:
                return
            try:
                if not client.is_connected():
                    return
            except:
                return
            
            async with pre_fetch_semaphore:
                try:
                    msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                    if msg:
                        account_messages[acc_id] = msg
                except:
                    pass  # 失败不记录，点击时会重试
        
        # 并发预获取所有消息
        pre_fetch_tasks = [pre_fetch_message(acc) for acc, _, _, _ in matched_accounts]
        await asyncio.gather(*pre_fetch_tasks, return_exceptions=True)
        print(f"[点击任务] ✅ 预获取完成，成功获取 {len(account_messages)}/{len(matched_accounts)} 个消息对象")

        # 并发控制：提高并发数到20（在防封前提下最大化性能）
        click_semaphore = asyncio.Semaphore(20)
        success_count = 0
        fail_count = 0
        success_accounts = []  # 记录成功的账号
        fail_accounts = []  # 记录失败的账号

        async def click_with_account(acc, btn_row, btn_col, btn_text, index):
            nonlocal success_count, fail_count, success_accounts, fail_accounts
            acc_id = acc['id']
            acc_name = acc.get('username') or acc.get('phone') or f"#{acc_id}"

            async with click_semaphore:
                # 获取账号客户端
                client = manager.account_clients.get(acc_id)
                if not client:
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: 客户端不存在")
                    return

                # 检查客户端是否真正连接
                try:
                    if not client.is_connected():
                        fail_count += 1
                        fail_accounts.append(f"{acc_name}: 客户端未连接")
                        return
                except Exception:
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: 连接状态异常")
                    return

                try:
                    # 使用预获取的消息对象，如果没有则重新获取
                    acc_msg = account_messages.get(acc_id)
                    if not acc_msg:
                        try:
                            acc_msg = await client.get_messages(target_chat_id, ids=target_msg_id)
                            if not acc_msg:
                                raise Exception('消息不存在或账号无法访问该消息')
                        except Exception as e:
                            fail_count += 1
                            error_str = str(e)
                            if 'CHANNEL_PRIVATE' in error_str or 'CHAT_FORBIDDEN' in error_str or 'USER_BANNED_IN_CHANNEL' in error_str:
                                error_msg = '未加入群组/频道或已被禁止'
                            elif 'MESSAGE_NOT_FOUND' in error_str or 'MSG_ID_INVALID' in error_str:
                                error_msg = '消息不存在或无效'
                            else:
                                error_msg = error_str[:50]
                            fail_accounts.append(f"{acc_name}: {error_msg}")
                            return

                    # 直接点击按钮（消息对象已准备好）
                    try:
                        await acc_msg.click(btn_row, btn_col)
                        success_count += 1
                        success_accounts.append(acc_name)
                    except Exception as e:
                        fail_count += 1
                        fail_accounts.append(f"{acc_name}: {str(e)[:50]}")
                except Exception as e:
                    fail_count += 1
                    fail_accounts.append(f"{acc_name}: {str(e)[:50]}")

        # 优化：将点击账号分成多个批次，每批次并发执行
        # 提高批次大小到20，充分利用CPU和内存
        accounts_per_batch = 20
        total_accounts = len(matched_accounts)
        num_batches = max(1, (total_accounts + accounts_per_batch - 1) // accounts_per_batch)

        print(f"[点击任务] 🎯 开始执行点击，共 {total_accounts} 个账号需要点击，分成 {num_batches} 个批次（每批次约 {accounts_per_batch} 个账号）")

        # 将账号列表分成多个批次
        account_batches = []
        for i in range(0, total_accounts, accounts_per_batch):
            batch = matched_accounts[i:i + accounts_per_batch]
            account_batches.append(batch)

        # 定义批次点击函数
        async def click_batch(batch_accounts, batch_index):
            """执行一个批次的点击任务（并发）"""
            try:
                batch_tasks = [click_with_account(acc, btn_row, btn_col, btn_text, idx)
                               for idx, (acc, btn_row, btn_col, btn_text) in enumerate(batch_accounts, start=batch_index * accounts_per_batch)]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                batch_success = sum(1 for r in batch_results if not isinstance(r, Exception))
                batch_fail = len(batch_results) - batch_success
                print(f"[点击批次 #{batch_index + 1}] 完成: 成功 {batch_success} 个，失败 {batch_fail} 个")
                return batch_results
            except Exception as e:
                print(f"[点击批次 #{batch_index + 1}] 执行出错: {e}")
                return []

        # 所有批次并发执行（充分利用CPU和内存）
        batch_tasks = [click_batch(batch, idx) for idx, batch in enumerate(account_batches)]
        all_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        # 展平所有批次的结果
        results = []
        for batch_results in all_results:
            if isinstance(batch_results, list):
                results.extend(batch_results)
            elif isinstance(batch_results, Exception):
                print(f"[点击任务] ⚠️ 批次执行异常: {batch_results}")

        print(f"[点击任务] 所有点击任务执行完成，共处理 {len(results)} 个结果")

        # 检查是否有异常
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[点击任务] ⚠️ 任务 {i} 执行异常: {result}")

        # 发送完成报告
        try:
            # 构建详细的完成报告
            total_accounts = len(accounts)
            matched_count = len(matched_accounts)
            all_btn_texts = [bt[2] for bt in button_positions]

            # 格式化 Chat ID 显示
            chat_id_display = actual_chat_id if actual_chat_id is not None else target_chat_id
            if isinstance(chat_id_display, str):
                # 如果是用户名，尝试显示为 @username 格式
                chat_id_display = f"@{chat_id_display}" if not chat_id_display.startswith('@') else chat_id_display
            else:
                # 如果是数字，直接显示
                chat_id_display = str(chat_id_display)
            
            report_msg = (
                f'✅ **点击任务完成**\\n'
                f'━━━━━━━━━━━━━━━━\\n'
                f'📋 **消息信息**\\n'
                f'• Chat ID: `{chat_id_display}`\\n'
                f'• Message ID: `{target_msg_id}`\\n'
                f'• 按钮文本: {", ".join(all_btn_texts[:3])}{"..." if len(all_btn_texts) > 3 else ""}\\n\\n'
                f'📊 **执行统计**\\n'
                f'• 总账号数: {total_accounts} 个\\n'
                f'• 匹配账号数: {matched_count} 个\\n'
                f'• ✅ 成功: {success_count} 个\\n'
                f'• ❌ 失败: {fail_count} 个\\n'
            )

            # 显示成功的账号
            if success_accounts:
                report_msg += f'\\n✅ **成功账号** ({len(success_accounts)} 个):\\n'
                for acc in success_accounts:
                    report_msg += f'• {acc}\\n'

            # 显示失败的账号
            if fail_accounts:
                report_msg += f'\\n❌ **失败账号** ({len(fail_accounts)} 个):\\n'
                for acc_info in fail_accounts[:10]:  # 最多显示10个
                    report_msg += f'• {acc_info}\\n'
                if len(fail_accounts) > 10:
                    report_msg += f'• ... 还有 {len(fail_accounts) - 10} 个失败\\n'

            await bot.send_message(report_chat_id, report_msg, parse_mode='markdown')
        except Exception as send_error:
            print(f"[点击任务] ⚠️ 发送完成报告失败: {send_error}")
    except Exception as e:
        print(f"[点击任务] ❌ 任务出错: {e}")
        import traceback
        traceback.print_exc()
        try:
            error_detail = (
                f'❌ **点击任务执行出错**\\n'
                f'━━━━━━━━━━━━━━━━\\n'
                f'错误类型：`{type(e).__name__}`\\n'
                f'错误信息：`{str(e)}`\\n\\n'
                f'请检查日志获取更多信息。'
            )
            await bot.send_message(report_chat_id, error_detail, parse_mode='markdown')
        except Exception as send_error:
            print(f"[点击任务] ❌ 发送错误消息也失败: {send_error}")


