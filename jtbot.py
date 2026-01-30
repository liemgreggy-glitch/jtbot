#!/usr/bin/env python3
"""
JTBot - Telegram 关键词监控机器人
多账号监控系统 - 完整功能版本
"""

import asyncio
import csv
import glob
import json
import logging
import os
import random
import re
import time
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, BufferedInputFile, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from cachetools import TTLCache
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError, PeerIdInvalidError
from telethon.tl.types import User, Channel, Chat, MessageEntityMention
import socks

# 加载环境变量
load_dotenv()

# ===== 日志配置 =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('jtbot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('jtbot')

# 降低 Telethon 日志级别，只显示警告及以上
logging.getLogger('telethon').setLevel(logging.WARNING)


# ===== 配置管理 =====
class Config:
    """配置管理类"""
    
    # Telegram API
    API_ID = int(os.getenv('API_ID', '0'))
    API_HASH = os.getenv('API_HASH', '')
    PHONE = os.getenv('PHONE', '')
    
    # Bot 配置
    BOT_TOKEN = os.getenv('BOT_TOKEN', '')
    ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '0'))
    MONITOR_CHAT_ID = int(os.getenv('MONITOR_CHAT_ID', '0'))
    
    # 文件路径
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_DIR = os.path.join(BASE_DIR, 'config')
    KEYWORDS_FILE = os.path.join(CONFIG_DIR, 'keywords.json')
    ACCOUNTS_FILE = os.path.join(CONFIG_DIR, 'accounts.json')
    RECORDS_FILE = os.path.join(CONFIG_DIR, 'records.json')
    FILTER_SETTINGS_FILE = os.path.join(CONFIG_DIR, 'filter_settings.json')
    BLACKLIST_FILE = os.path.join(CONFIG_DIR, 'blacklist.json')
    PROXY_FILE = os.path.join(BASE_DIR, 'proxy.txt')
    SESSIONS_DIR = os.path.join(BASE_DIR, 'sessions')
    EXPORTS_DIR = os.path.join(BASE_DIR, 'exports')
    SESSION_NAME = 'jtbot_session'  # Legacy session name
    
    # DM Pool 相关路径
    DM_SESSIONS_DIR = os.path.join(BASE_DIR, 'dm_sessions')
    DM_ACCOUNTS_FILE = os.path.join(CONFIG_DIR, 'dm_accounts.json')
    DM_SETTINGS_FILE = os.path.join(CONFIG_DIR, 'dm_settings.json')
    DM_TEMPLATES_FILE = os.path.join(CONFIG_DIR, 'dm_templates.json')
    DM_RECORDS_FILE = os.path.join(CONFIG_DIR, 'dm_records.json')
    DM_SENT_USERS_FILE = os.path.join(CONFIG_DIR, 'dm_sent_users.json')
    
    @classmethod
    def validate(cls):
        """验证配置 - 简化版，不再要求 PHONE"""
        errors = []
        if not cls.API_ID or cls.API_ID == 0:
            errors.append('API_ID 未配置')
        if not cls.API_HASH:
            errors.append('API_HASH 未配置')
        if not cls.BOT_TOKEN:
            errors.append('BOT_TOKEN 未配置')
        if not cls.ADMIN_USER_ID or cls.ADMIN_USER_ID == 0:
            errors.append('ADMIN_USER_ID 未配置')
        if not cls.MONITOR_CHAT_ID or cls.MONITOR_CHAT_ID == 0:
            errors.append('MONITOR_CHAT_ID 未配置')
        
        if errors:
            raise ValueError('配置错误:\n' + '\n'.join(errors))
        return True


# ===== 代理解析 =====
class ProxyParser:
    """代理配置解析器"""
    
    @staticmethod
    def parse_proxy(proxy_str: str) -> Optional[Dict]:
        """
        解析代理字符串，支持多种格式
        
        支持格式:
        - socks5://127.0.0.1:1080
        - socks5://user:pass@127.0.0.1:1080
        - http://127.0.0.1:8080
        - http://user:pass@127.0.0.1:8080
        - 127.0.0.1:1080
        - 127.0.0.1:1080:user:pass
        - user:pass@127.0.0.1:1080
        - socks5h://127.0.0.1:1080
        """
        proxy_str = proxy_str.strip()
        if not proxy_str or proxy_str.startswith('#'):
            return None
        
        try:
            # 格式1: socks5://127.0.0.1:1080 或 http://127.0.0.1:8080
            if '://' in proxy_str:
                parsed = urlparse(proxy_str)
                proxy_type = parsed.scheme.replace('socks5h', 'socks5')
                
                if proxy_type not in ['socks5', 'http', 'https']:
                    return None
                
                proxy_type_code = socks.SOCKS5 if proxy_type == 'socks5' else socks.HTTP
                
                return {
                    'proxy_type': proxy_type_code,
                    'addr': parsed.hostname,
                    'port': parsed.port,
                    'username': parsed.username,
                    'password': parsed.password,
                    'rdns': True
                }
            
            # 格式2: user:pass@127.0.0.1:1080
            if '@' in proxy_str:
                auth, addr = proxy_str.split('@', 1)
                username, password = auth.split(':', 1)
                host, port = addr.rsplit(':', 1)
                
                return {
                    'proxy_type': socks.SOCKS5,
                    'addr': host,
                    'port': int(port),
                    'username': username,
                    'password': password,
                    'rdns': True
                }
            
            # 格式3: 127.0.0.1:1080:user:pass
            parts = proxy_str.split(':')
            if len(parts) == 4:
                return {
                    'proxy_type': socks.SOCKS5,
                    'addr': parts[0],
                    'port': int(parts[1]),
                    'username': parts[2],
                    'password': parts[3],
                    'rdns': True
                }
            
            # 格式4: 127.0.0.1:1080
            if len(parts) == 2:
                return {
                    'proxy_type': socks.SOCKS5,
                    'addr': parts[0],
                    'port': int(parts[1]),
                    'username': None,
                    'password': None,
                    'rdns': True
                }
        
        except Exception as e:
            # Sanitize proxy string to avoid logging credentials
            safe_proxy = proxy_str.split('@')[-1] if '@' in proxy_str else proxy_str
            logger.error(f'代理解析失败 [{safe_proxy}]: {e}')
        
        return None
    
    @staticmethod
    def load_proxy_from_file(filepath: str) -> Optional[Dict]:
        """从文件加载代理配置"""
        if not os.path.exists(filepath):
            logger.warning(f'代理配置文件不存在: {filepath}')
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    proxy = ProxyParser.parse_proxy(line)
                    if proxy:
                        logger.info(f'加载代理: {proxy["addr"]}:{proxy["port"]}')
                        return proxy
        except Exception as e:
            logger.error(f'读取代理配置文件失败: {e}')
        
        return None


# ===== 关键词管理 =====
class KeywordManager:
    """关键词管理器"""
    
    def __init__(self, keywords_file: str):
        self.keywords_file = keywords_file
        self.keywords: List[str] = []
        self.load_keywords()
    
    def load_keywords(self):
        """加载关键词"""
        try:
            if os.path.exists(self.keywords_file):
                with open(self.keywords_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.keywords = data.get('keywords', [])
                    logger.info(f'加载了 {len(self.keywords)} 个关键词')
            else:
                self.keywords = []
                self.save_keywords()
        except Exception as e:
            logger.error(f'加载关键词失败: {e}')
            self.keywords = []
    
    def save_keywords(self):
        """保存关键词"""
        try:
            with open(self.keywords_file, 'w', encoding='utf-8') as f:
                json.dump({'keywords': self.keywords}, f, ensure_ascii=False, indent=2)
            logger.info(f'保存了 {len(self.keywords)} 个关键词')
        except Exception as e:
            logger.error(f'保存关键词失败: {e}')
    
    def add_keywords(self, keywords: List[str]) -> int:
        """添加关键词"""
        added = 0
        for keyword in keywords:
            keyword = keyword.strip()
            # 检查关键词长度 ≤ 10个字符
            if len(keyword) > 10:
                logger.warning(f"关键词过长(>{len(keyword)}字符)，已忽略: {keyword}")
                continue
            if keyword and keyword not in self.keywords:
                self.keywords.append(keyword)
                added += 1
        if added > 0:
            self.save_keywords()
        return added
    
    def remove_keyword(self, keyword: str) -> bool:
        """删除关键词"""
        if keyword in self.keywords:
            self.keywords.remove(keyword)
            self.save_keywords()
            return True
        return False
    
    def get_keywords(self) -> List[str]:
        """获取所有关键词"""
        return self.keywords.copy()
    
    def match(self, text: str) -> List[str]:
        """匹配关键词 - 优化版本，使用预处理的小写文本"""
        if not text:
            return []
        
        # 预处理：只转换一次
        text_lower = text.lower()
        matched = []
        for keyword in self.keywords:
            if keyword.lower() in text_lower:
                matched.append(keyword)
        
        return matched


# ===== 账号管理 =====
class AccountManager:
    """多账号管理器"""
    
    def __init__(self, accounts_file: str):
        self.accounts_file = accounts_file
        self.accounts: List[Dict] = []
        self.max_accounts = 10
        self.load_accounts()
    
    def load_accounts(self):
        """加载账号列表"""
        try:
            if os.path.exists(self.accounts_file):
                with open(self.accounts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.accounts = data.get('accounts', [])
                    self.max_accounts = data.get('max_accounts', 10)
                    logger.info(f'加载了 {len(self.accounts)} 个监控账号')
            else:
                self.accounts = []
                self.save_accounts()
        except Exception as e:
            logger.error(f'加载账号失败: {e}')
            self.accounts = []
    
    def save_accounts(self):
        """保存账号列表"""
        try:
            with open(self.accounts_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'accounts': self.accounts,
                    'max_accounts': self.max_accounts
                }, f, ensure_ascii=False, indent=2)
            logger.info(f'保存了 {len(self.accounts)} 个账号')
        except Exception as e:
            logger.error(f'保存账号失败: {e}')
    
    def add_account(self, phone: str, session_file: str, name: str, username: str, user_id: int) -> bool:
        """添加账号"""
        if len(self.accounts) >= self.max_accounts:
            return False
        
        # 检查是否已存在
        if any(acc['phone'] == phone for acc in self.accounts):
            return False
        
        account = {
            'phone': phone,
            'session_file': session_file,
            'name': name,
            'username': username,
            'user_id': user_id,
            'enabled': True,
            'added_at': datetime.now().isoformat()
        }
        
        self.accounts.append(account)
        self.save_accounts()
        return True
    
    def remove_account(self, phone: str) -> bool:
        """删除账号"""
        for i, acc in enumerate(self.accounts):
            if acc['phone'] == phone:
                self.accounts.pop(i)
                self.save_accounts()
                return True
        return False
    
    def get_account(self, phone: str) -> Optional[Dict]:
        """获取账号信息"""
        for acc in self.accounts:
            if acc['phone'] == phone:
                return acc
        return None
    
    def get_all_accounts(self) -> List[Dict]:
        """获取所有账号"""
        return self.accounts.copy()
    
    def update_account_status(self, phone: str, enabled: bool):
        """更新账号状态"""
        for acc in self.accounts:
            if acc['phone'] == phone:
                acc['enabled'] = enabled
                self.save_accounts()
                break


# ===== 过滤设置管理 =====
class FilterManager:
    """过滤设置管理器"""
    
    def __init__(self, settings_file: str):
        self.settings_file = settings_file
        self.settings = {
            'cooldown_minutes': 5,
            'max_message_length': 100,  # 消息长度限制（字符数）
            'filter_no_username': True,
            'filter_no_avatar': False,
            'min_account_age_days': 7
        }
        self.load_settings()
    
    def load_settings(self):
        """加载设置"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    self.settings.update(json.load(f))
                logger.info('加载过滤设置成功')
            else:
                self.save_settings()
        except Exception as e:
            logger.error(f'加载过滤设置失败: {e}')
    
    def save_settings(self):
        """保存设置"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            logger.info('保存过滤设置成功')
        except Exception as e:
            logger.error(f'保存过滤设置失败: {e}')
    
    def get_setting(self, key: str):
        """获取设置值"""
        return self.settings.get(key)
    
    def update_setting(self, key: str, value):
        """更新设置值"""
        self.settings[key] = value
        self.save_settings()
    
    def check_user_filter(self, user: User) -> Tuple[bool, str]:
        """
        检查用户是否通过过滤
        返回: (是否通过, 原因)
        """
        # 检查是否有用户名
        if self.settings['filter_no_username'] and not user.username:
            return False, '无用户名'
        
        # 检查是否有头像
        if self.settings['filter_no_avatar'] and not user.photo:
            return False, '无头像'
        
        # 检查账号年龄（基于user_id估算）
        min_age_days = self.settings['min_account_age_days']
        if min_age_days > 0:
            # Telegram user_id 大致与创建时间相关
            # 这是一个粗略的估计
            account_age_days = self._estimate_account_age(user.id)
            if account_age_days < min_age_days:
                return False, f'账号年龄不足{min_age_days}天'
        
        return True, ''
    
    def _estimate_account_age(self, user_id: int) -> int:
        """估算账号年龄（天数）- 基于user_id"""
        # 这是一个粗略估计，基于Telegram的user_id分配规律
        # 较小的ID通常表示较早注册
        # 这里简化处理，实际可以更复杂
        if user_id < 1000000000:  # 10亿以下，认为是老账号
            return 365 * 5  # 5年以上
        elif user_id < 2000000000:  # 20亿以下
            return 365 * 2  # 2年以上
        elif user_id < 5000000000:  # 50亿以下
            return 180  # 半年以上
        else:
            return 30  # 较新账号


# ===== 记录管理 =====
class RecordManager:
    """触发记录管理器"""
    
    def __init__(self, records_file: str):
        self.records_file = records_file
        self.records: List[Dict] = []
        self.load_records()
    
    def load_records(self):
        """加载记录"""
        try:
            if os.path.exists(self.records_file):
                with open(self.records_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.records = data.get('records', [])
                logger.info(f'加载了 {len(self.records)} 条记录')
            else:
                self.records = []
                self.save_records()
        except Exception as e:
            logger.error(f'加载记录失败: {e}')
            self.records = []
    
    def save_records(self):
        """保存记录"""
        try:
            with open(self.records_file, 'w', encoding='utf-8') as f:
                json.dump({'records': self.records}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f'保存记录失败: {e}')
    
    def add_record(self, user_id: int, username: str, name: str, chat_id: int, 
                   chat_title: str, keyword: str, message: str, monitor_account: str):
        """添加触发记录"""
        record = {
            'user_id': user_id,
            'username': username,
            'name': name,
            'chat_id': chat_id,
            'chat_title': chat_title,
            'keyword': keyword,
            'message': message,
            'time': datetime.now().isoformat(),
            'monitor_account': monitor_account
        }
        self.records.append(record)
        
        # 限制记录数量，避免文件过大
        if len(self.records) > 10000:
            self.records = self.records[-10000:]
        
        self.save_records()
    
    def get_recent_records(self, limit: int = 100) -> List[Dict]:
        """获取最近的记录"""
        return self.records[-limit:]
    
    def export_user_list(self) -> str:
        """导出用户列表（简洁格式）"""
        users = {}
        for record in self.records:
            user_id = record['user_id']
            if user_id not in users:
                users[user_id] = {
                    'username': record['username'],
                    'user_id': user_id
                }
        
        output = "用户名,用户ID\n"
        for user_id, user_data in users.items():
            username = user_data['username'] or '无'
            output += f"{username},{user_id}\n"
        
        return output
    
    def export_full_records(self) -> str:
        """导出完整记录（CSV格式）"""
        output = "用户ID,用户名,昵称,来源群组,触发关键词,触发时间,消息内容\n"
        for record in self.records:
            # CSV格式，需要转义特殊字符
            user_id = str(record['user_id'])
            username = (record['username'] or '无').replace('"', '""')
            name = record['name'].replace('"', '""')
            chat_title = record['chat_title'].replace('"', '""')
            keyword = record['keyword'].replace('"', '""')
            time_str = record['time']
            message = record['message'].replace('"', '""').replace('\n', ' ')
            
            output += f'{user_id},"{username}","{name}","{chat_title}","{keyword}",{time_str},"{message}"\n'
        
        return output
    
    def filter_records(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, 
                      keywords: Optional[List[str]] = None) -> List[Dict]:
        """过滤记录"""
        filtered = self.records.copy()
        
        # 时间范围过滤
        if start_time or end_time:
            temp_filtered = []
            for record in filtered:
                try:
                    record_time = datetime.fromisoformat(record['time'])
                    if start_time and record_time < start_time:
                        continue
                    if end_time and record_time > end_time:
                        continue
                    temp_filtered.append(record)
                except:
                    continue
            filtered = temp_filtered
        
        # 关键词过滤
        if keywords:
            temp_filtered = []
            for record in filtered:
                if record.get('keyword') in keywords:
                    temp_filtered.append(record)
            filtered = temp_filtered
        
        return filtered


# ===== 黑名单管理 =====
class BlacklistManager:
    """黑名单管理器"""
    
    def __init__(self, blacklist_file: str):
        self.blacklist_file = blacklist_file
        self.users: List[Dict] = []
        self.chats: List[Dict] = []
        # 使用集合加速查找 (O(1) vs O(n))
        self._user_ids: set = set()
        self._chat_ids: set = set()
        self.load_blacklist()
    
    def load_blacklist(self):
        """加载黑名单"""
        try:
            if os.path.exists(self.blacklist_file):
                with open(self.blacklist_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.users = data.get('users', [])
                    self.chats = data.get('chats', [])
                # 重建查找集合
                self._user_ids = {u['user_id'] for u in self.users}
                self._chat_ids = {c['chat_id'] for c in self.chats}
                logger.info(f'加载黑名单: {len(self.users)}个用户, {len(self.chats)}个群组')
            else:
                self.users = []
                self.chats = []
                self._user_ids = set()
                self._chat_ids = set()
                self.save_blacklist()
        except Exception as e:
            logger.error(f'加载黑名单失败: {e}')
            self.users = []
            self.chats = []
            self._user_ids = set()
            self._chat_ids = set()
    
    def save_blacklist(self):
        """保存黑名单"""
        try:
            with open(self.blacklist_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'users': self.users,
                    'chats': self.chats
                }, f, ensure_ascii=False, indent=2)
            logger.info('保存黑名单成功')
        except Exception as e:
            logger.error(f'保存黑名单失败: {e}')
    
    def add_user(self, user_id: int, username: str = '') -> bool:
        """添加用户到黑名单"""
        # 使用集合快速检查
        if user_id in self._user_ids:
            return False
        
        self.users.append({
            'user_id': user_id,
            'username': username,
            'blocked_at': datetime.now().isoformat()
        })
        self._user_ids.add(user_id)
        self.save_blacklist()
        return True
    
    def add_chat(self, chat_id: int, title: str = '') -> bool:
        """添加群组到黑名单"""
        # 使用集合快速检查
        if chat_id in self._chat_ids:
            return False
        
        self.chats.append({
            'chat_id': chat_id,
            'title': title,
            'blocked_at': datetime.now().isoformat()
        })
        self._chat_ids.add(chat_id)
        self.save_blacklist()
        return True
    
    def remove_user(self, user_id: int) -> bool:
        """从黑名单移除用户"""
        for i, user in enumerate(self.users):
            if user['user_id'] == user_id:
                self.users.pop(i)
                self._user_ids.discard(user_id)
                self.save_blacklist()
                return True
        return False
    
    def remove_chat(self, chat_id: int) -> bool:
        """从黑名单移除群组"""
        for i, chat in enumerate(self.chats):
            if chat['chat_id'] == chat_id:
                self.chats.pop(i)
                self._chat_ids.discard(chat_id)
                self.save_blacklist()
                return True
        return False
    
    def is_user_blocked(self, user_id: int) -> bool:
        """检查用户是否在黑名单 - O(1) 查找"""
        return user_id in self._user_ids
    
    def is_chat_blocked(self, chat_id: int) -> bool:
        """检查群组是否在黑名单 - O(1) 查找"""
        return chat_id in self._chat_ids
    
    def clear_users(self):
        """清空用户黑名单"""
        self.users = []
        self._user_ids.clear()
        self.save_blacklist()
    
    def clear_chats(self):
        """清空群组黑名单"""
        self.chats = []
        self._chat_ids.clear()
        self.save_blacklist()
    
    def get_users(self) -> List[Dict]:
        """获取用户黑名单"""
        return self.users.copy()
    
    def get_chats(self) -> List[Dict]:
        """获取群组黑名单"""
        return self.chats.copy()


# ===== 私信号池管理 =====
class DMAccountManager:
    """私信号池管理器"""
    
    # 状态检测模式匹配
    STATUS_PATTERNS = {
        # 地理限制提示 - 判定为无限制（优先级最高）
        "geo_warning": [
            "some phone numbers may trigger a harsh response",
            "phone numbers may trigger",
        ],
        "active": [
            "good news, no limits are currently applied",
            "you're free as a bird",
            "no limits",
            "free as a bird",
            "no restrictions",
            "all good",
            "account is free",
            "not limited",
            "正常",
            "没有限制",
            "无限制"
        ],
        "restricted": [
            "account is now limited until",
            "limited until",
            "moderators have confirmed the report",
            "users found your messages annoying",
            "will be automatically released",
            "temporarily limited",
            "暂时限制",
            "临时限制"
        ],
        "spam": [
            "actions can trigger a harsh response from our anti-spam systems",
            "account was limited",
            "you will not be able to send messages",
            "违规",
        ],
        "banned": [
            "permanently banned",
            "account has been frozen permanently",
            "permanently restricted",
            "banned permanently",
            "blocked for violations",
            "terms of service",
            "banned",
            "suspended",
            "永久限制",
            "永久封禁"
        ],
        "frozen": [
            "wait",
            "pending",
            "verification",
            "等待",
            "审核中"
        ]
    }
    
    # 多语言翻译（俄文/中文→英文）
    TRANSLATIONS = {
        'ограничения': 'limitations',
        'заблокирован': 'blocked',
        'хорошие новости': 'good news',
        'нет ограничений': 'no limits',
        '正常': 'all good',
        '没有限制': 'no limits',
        '永久封禁': 'permanently banned',
        '限制': 'limited',
        '暂时': 'temporarily',
        '验证': 'verification',
    }
    
    def __init__(self, accounts_file: str):
        self.accounts_file = accounts_file
        self.accounts: List[Dict] = []
        self.load_accounts()
    
    def load_accounts(self):
        """加载私信号账号列表"""
        try:
            if os.path.exists(self.accounts_file):
                with open(self.accounts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.accounts = data.get('accounts', [])
                    logger.info(f'加载了 {len(self.accounts)} 个私信号')
            else:
                self.accounts = []
                self.save_accounts()
        except Exception as e:
            logger.error(f'加载私信号失败: {e}')
            self.accounts = []
    
    def save_accounts(self):
        """保存私信号账号列表"""
        try:
            with open(self.accounts_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'accounts': self.accounts,
                    'last_updated': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
            logger.info(f'保存了 {len(self.accounts)} 个私信号')
        except Exception as e:
            logger.error(f'保存私信号失败: {e}')
    
    def add_account(self, phone: str, session_file: str, name: str, username: str, 
                   user_id: int, status: str = 'unknown', connection_type: str = 'unknown') -> bool:
        """添加私信号"""
        # 检查是否已存在
        if any(acc['phone'] == phone for acc in self.accounts):
            # 更新现有账号
            for acc in self.accounts:
                if acc['phone'] == phone:
                    acc.update({
                        'name': name,
                        'username': username,
                        'user_id': user_id,
                        'status': status,
                        'connection_type': connection_type,
                        'updated_at': datetime.now().isoformat()
                    })
                    break
            self.save_accounts()
            return True
        
        account = {
            'phone': phone,
            'session_file': session_file,
            'name': name,
            'username': username,
            'user_id': user_id,
            'status': status,  # active/restricted/spam/banned/frozen/failed
            'can_send_dm': status == 'active',
            'connection_type': connection_type,  # proxy/local/failed
            'daily_sent': 0,
            'last_sent_date': None,
            'added_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        self.accounts.append(account)
        self.save_accounts()
        return True
    
    def remove_account(self, phone: str) -> bool:
        """删除私信号"""
        for i, acc in enumerate(self.accounts):
            if acc['phone'] == phone:
                self.accounts.pop(i)
                self.save_accounts()
                return True
        return False
    
    def get_account(self, phone: str) -> Optional[Dict]:
        """获取账号信息"""
        for acc in self.accounts:
            if acc['phone'] == phone:
                return acc
        return None
    
    def get_all_accounts(self) -> List[Dict]:
        """获取所有账号"""
        return self.accounts.copy()
    
    def get_available_accounts(self, daily_limit: int = 50) -> List[Dict]:
        """获取可用的私信号（状态为active且未超过日限额）"""
        today = datetime.now().date().isoformat()
        available = []
        
        for acc in self.accounts:
            if acc.get('status') != 'active' or not acc.get('can_send_dm', False):
                continue
            
            # 检查日限额
            last_sent_date = acc.get('last_sent_date')
            daily_sent = acc.get('daily_sent', 0)
            
            # 如果是新的一天，重置计数
            if last_sent_date != today:
                acc['daily_sent'] = 0
                acc['last_sent_date'] = today
                daily_sent = 0
            
            if daily_sent < daily_limit:
                available.append(acc)
        
        return available
    
    def update_account_status(self, phone: str, status: str, can_send_dm: bool = None):
        """更新账号状态"""
        for acc in self.accounts:
            if acc['phone'] == phone:
                acc['status'] = status
                if can_send_dm is not None:
                    acc['can_send_dm'] = can_send_dm
                else:
                    acc['can_send_dm'] = (status == 'active')
                acc['updated_at'] = datetime.now().isoformat()
                self.save_accounts()
                break
    
    def increment_sent_count(self, phone: str):
        """增加发送计数"""
        today = datetime.now().date().isoformat()
        for acc in self.accounts:
            if acc['phone'] == phone:
                if acc.get('last_sent_date') != today:
                    acc['daily_sent'] = 0
                    acc['last_sent_date'] = today
                acc['daily_sent'] = acc.get('daily_sent', 0) + 1
                self.save_accounts()
                break
    
    def translate_text(self, text: str) -> str:
        """翻译文本（俄文/中文→英文）"""
        text_lower = text.lower()
        for src, dst in self.TRANSLATIONS.items():
            if src in text_lower:
                text_lower = text_lower.replace(src, dst)
        return text_lower
    
    def detect_status_from_spambot(self, message_text: str) -> Tuple[str, bool]:
        """
        从 @SpamBot 的回复中检测账号状态
        返回: (status, can_send_dm)
        """
        # 翻译消息
        translated = self.translate_text(message_text)
        
        # 优先检查地理限制提示（判定为active）
        for pattern in self.STATUS_PATTERNS['geo_warning']:
            if pattern.lower() in translated:
                return 'active', True
        
        # 检查无限制
        for pattern in self.STATUS_PATTERNS['active']:
            if pattern.lower() in translated:
                return 'active', True
        
        # 检查临时限制
        for pattern in self.STATUS_PATTERNS['restricted']:
            if pattern.lower() in translated:
                return 'restricted', False
        
        # 检查垃圾邮件限制
        for pattern in self.STATUS_PATTERNS['spam']:
            if pattern.lower() in translated:
                return 'spam', False
        
        # 检查永久封禁
        for pattern in self.STATUS_PATTERNS['banned']:
            if pattern.lower() in translated:
                return 'banned', False
        
        # 检查等待验证
        for pattern in self.STATUS_PATTERNS['frozen']:
            if pattern.lower() in translated:
                return 'frozen', False
        
        # 默认返回未知状态
        return 'unknown', False
    
    async def check_account_status(self, client: TelegramClient) -> Tuple[str, bool]:
        """
        通过与 @SpamBot 对话检测账号状态
        返回: (status, can_send_dm)
        """
        try:
            # 发送消息给 @SpamBot
            await client.send_message('@SpamBot', '/start')
            await asyncio.sleep(2)
            
            # 获取最新消息
            messages = await client.get_messages('@SpamBot', limit=1)
            if messages and len(messages) > 0:
                response_text = messages[0].text
                return self.detect_status_from_spambot(response_text)
            
            return 'unknown', False
            
        except Exception as e:
            logger.error(f'检测账号状态失败: {e}')
            return 'failed', False
    
    def get_status_emoji(self, status: str) -> str:
        """获取状态对应的 Emoji"""
        emoji_map = {
            'active': '✅',
            'restricted': '⚠️',
            'spam': '📵',
            'banned': '🚫',
            'frozen': '❄️',
            'failed': '🔌',
            'unknown': '❓'
        }
        return emoji_map.get(status, '❓')
    
    def get_connection_emoji(self, conn_type: str) -> str:
        """获取连接类型对应的 Emoji"""
        emoji_map = {
            'proxy': '🟢',
            'local': '🟡',
            'failed': '🔴',
            'unknown': '⚪'
        }
        return emoji_map.get(conn_type, '⚪')


class DMTemplateManager:
    """私信话术管理器"""
    
    def __init__(self, templates_file: str):
        self.templates_file = templates_file
        self.templates: List[Dict] = []
        self.load_templates()
    
    def load_templates(self):
        """加载话术模板"""
        try:
            if os.path.exists(self.templates_file):
                with open(self.templates_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.templates = data.get('templates', [])
                    logger.info(f'加载了 {len(self.templates)} 个话术模板')
            else:
                self.templates = []
                self.save_templates()
        except Exception as e:
            logger.error(f'加载话术模板失败: {e}')
            self.templates = []
    
    def save_templates(self):
        """保存话术模板"""
        try:
            with open(self.templates_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'templates': self.templates
                }, f, ensure_ascii=False, indent=2)
            logger.info(f'保存了 {len(self.templates)} 个话术模板')
        except Exception as e:
            logger.error(f'保存话术模板失败: {e}')
    
    def add_template(self, template_type: str, content: Dict) -> int:
        """
        添加话术模板
        template_type: text/postbot/forward/forward_hidden
        content: 根据类型不同，包含不同字段
        """
        template_id = len(self.templates) + 1
        template = {
            'id': template_id,
            'type': template_type,
            'content': content,
            'created_at': datetime.now().isoformat()
        }
        self.templates.append(template)
        self.save_templates()
        return template_id
    
    def remove_template(self, template_id: int) -> bool:
        """删除话术模板"""
        for i, tpl in enumerate(self.templates):
            if tpl['id'] == template_id:
                self.templates.pop(i)
                self.save_templates()
                return True
        return False
    
    def get_template(self, template_id: int) -> Optional[Dict]:
        """获取话术模板"""
        for tpl in self.templates:
            if tpl['id'] == template_id:
                return tpl
        return None
    
    def get_all_templates(self) -> List[Dict]:
        """获取所有话术模板"""
        return self.templates.copy()
    
    def get_random_template(self) -> Optional[Dict]:
        """随机获取一个话术模板"""
        if not self.templates:
            return None
        return random.choice(self.templates)
    
    @staticmethod
    def process_spintax(text: str) -> str:
        """
        处理 Spintax 变体语法
        例如: {你好|您好|Hi} -> 随机选择一个
        """
        pattern = r'\{([^}]+)\}'
        
        def replace_choice(match):
            choices = match.group(1).split('|')
            return random.choice(choices)
        
        return re.sub(pattern, replace_choice, text)
    
    @staticmethod
    def add_random_emoji(text: str) -> str:
        """在文末添加随机 Emoji"""
        emojis = ['😊', '👋', '✨', '🌟', '💫', '🎯', '🔥', '💪', '👍', '🙏']
        return f"{text} {random.choice(emojis)}"
    
    @staticmethod
    def add_invisible_timestamp(text: str) -> str:
        """添加不可见字符（完全不可见）"""
        import random
        
        zero_width_chars = [
            '\u200b',  # 零宽空格
            '\u200c',  # 零宽非连接符  
            '\u200d',  # 零宽连接符
            '\u2060',  # 词连接符
        ]
        
        length = random.randint(6, 10)
        invisible = ''.join(random.choice(zero_width_chars) for _ in range(length))
        
        return text + invisible
    
    def generate_text_variant(self, text: str, use_emoji: bool = True, 
                            use_timestamp: bool = True, use_synonym: bool = False) -> str:
        """
        生成文本变体
        """
        # 处理 Spintax 语法
        result = self.process_spintax(text)
        
        # 添加随机 Emoji
        if use_emoji:
            result = self.add_random_emoji(result)
        
        # 添加不可见时间戳
        if use_timestamp:
            result = self.add_invisible_timestamp(result)
        
        # TODO: 同义词替换（可选，需要同义词库）
        if use_synonym:
            pass
        
        return result


class DMRecordManager:
    """私信记录管理器"""
    
    def __init__(self, records_file: str, sent_users_file: str):
        self.records_file = records_file
        self.sent_users_file = sent_users_file
        self.records: List[Dict] = []
        self.sent_users: Dict[str, str] = {}  # 改为字典，key为用户ID字符串，value为时间戳
        self.load_records()
        self.load_sent_users()
    
    def load_records(self):
        """加载私信记录"""
        try:
            if os.path.exists(self.records_file):
                with open(self.records_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.records = data.get('records', [])
                    logger.info(f'加载了 {len(self.records)} 条私信记录')
            else:
                self.records = []
                self.save_records()
        except Exception as e:
            logger.error(f'加载私信记录失败: {e}')
            self.records = []
    
    def save_records(self):
        """保存私信记录"""
        try:
            with open(self.records_file, 'w', encoding='utf-8') as f:
                json.dump({'records': self.records}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f'保存私信记录失败: {e}')
    
    def load_sent_users(self):
        """加载已私信用户列表"""
        try:
            if os.path.exists(self.sent_users_file):
                with open(self.sent_users_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    sent_users_data = data.get('sent_users', {})
                    
                    # 兼容旧格式（列表）转换为新格式（字典）
                    if isinstance(sent_users_data, list):
                        # 旧格式，转换为新格式，默认时间为当前时间
                        self.sent_users = {str(uid): datetime.now().isoformat() for uid in sent_users_data}
                        self.save_sent_users()  # 保存新格式
                    else:
                        self.sent_users = sent_users_data
                    
                    logger.info(f'加载了 {len(self.sent_users)} 个已私信用户')
            else:
                self.sent_users = {}
                self.save_sent_users()
        except Exception as e:
            logger.error(f'加载已私信用户列表失败: {e}')
            self.sent_users = {}
    
    def save_sent_users(self):
        """保存已私信用户列表"""
        try:
            with open(self.sent_users_file, 'w', encoding='utf-8') as f:
                json.dump({'sent_users': self.sent_users}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f'保存已私信用户列表失败: {e}')
    
    def is_user_sent(self, user_id: int, reset_hours: int = 24) -> bool:
        """检查用户是否在指定时间内被私信过
        
        Args:
            user_id: 用户ID
            reset_hours: 重置时间（小时），默认24小时
        
        Returns:
            True: 用户在reset_hours内被私信过，不应再次私信
            False: 用户未被私信过或已超过reset_hours，可以私信
        """
        user_id_str = str(user_id)
        
        if user_id_str not in self.sent_users:
            return False
        
        try:
            # 检查是否超过重置时间
            sent_time = datetime.fromisoformat(self.sent_users[user_id_str])
            if datetime.now() - sent_time > timedelta(hours=reset_hours):
                # 超过重置时间，可以再次私信
                logger.info(f"用户 {user_id} 上次私信超过{reset_hours}小时，可以再次私信")
                return False
            
            return True
        except Exception as e:
            logger.error(f"检查用户私信时间失败: {e}")
            return False
    
    def add_sent_user(self, user_id: int):
        """添加用户到已私信列表（记录时间）"""
        user_id_str = str(user_id)
        self.sent_users[user_id_str] = datetime.now().isoformat()
        self.save_sent_users()
    
    def clear_sent_users(self):
        """清空已私信用户列表"""
        self.sent_users = {}
        self.save_sent_users()
        logger.info("已清空私信用户列表")
    
    def add_record(self, user_id: int, username: str, dm_account: str, 
                  template_id: int, template_type: str, status: str, 
                  error: str = None, error_text: str = None):
        """添加私信记录"""
        record = {
            'user_id': user_id,
            'username': username,
            'dm_account': dm_account,
            'template_id': template_id,
            'template_type': template_type,
            'status': status,  # success/failed
            'time': datetime.now().isoformat()
        }
        
        if error:
            record['error'] = error
            record['error_text'] = error_text or self.get_error_text(error)
        
        self.records.append(record)
        
        # 限制记录数量
        if len(self.records) > 10000:
            self.records = self.records[-10000:]
        
        self.save_records()
    
    @staticmethod
    def get_error_text(error_code: str) -> str:
        """获取错误文本"""
        error_map = {
            'USER_PRIVACY_RESTRICTED': '对方隐私设置禁止私信',
            'PEER_FLOOD': '发送频率限制',
            'USER_BANNED_IN_CHANNEL': '被频道封禁',
            'USER_IS_BOT': '对方是机器人',
            'CHAT_WRITE_FORBIDDEN': '无法发送消息',
            'SESSION_REVOKED': 'session已失效',
            'FLOOD_WAIT': '需要等待'
        }
        return error_map.get(error_code, '未知错误')
    
    def get_recent_records(self, limit: int = 100) -> List[Dict]:
        """获取最近的记录"""
        return self.records[-limit:]
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        today = datetime.now().date().isoformat()
        
        today_records = [r for r in self.records if r['time'].startswith(today)]
        success_count = sum(1 for r in today_records if r['status'] == 'success')
        failed_count = sum(1 for r in today_records if r['status'] == 'failed')
        
        return {
            'total_sent': len(today_records),
            'success': success_count,
            'failed': failed_count,
            'total_users': len(self.sent_users)
        }


class DMSettingsManager:
    """私信设置管理器"""
    
    def __init__(self, settings_file: str):
        self.settings_file = settings_file
        self.settings = {
            'enabled': True,
            'delay_min': 30,
            'delay_max': 120,
            'batch_size': 5,
            'batch_rest_min': 180,
            'batch_rest_max': 480,
            'daily_limit': 50,
            'active_hours_start': 9,
            'active_hours_end': 22,
            'send_sticker_first': False,  # 是否先发贴纸打招呼
            'sticker_delay_min': 1.0,     # 贴纸后延迟最小秒数
            'sticker_delay_max': 3.0      # 贴纸后延迟最大秒数
        }
        self.load_settings()
    
    def load_settings(self):
        """加载设置"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    self.settings.update(json.load(f))
                logger.info('加载私信设置成功')
            else:
                self.save_settings()
        except Exception as e:
            logger.error(f'加载私信设置失败: {e}')
    
    def save_settings(self):
        """保存设置"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            logger.info('保存私信设置成功')
        except Exception as e:
            logger.error(f'保存私信设置失败: {e}')
    
    def get_setting(self, key: str):
        """获取设置值"""
        return self.settings.get(key)
    
    def update_setting(self, key: str, value):
        """更新设置值"""
        self.settings[key] = value
        self.save_settings()
    
    def is_active_hour(self) -> bool:
        """检查当前是否在活跃时段"""
        current_hour = datetime.now().hour
        start_hour = self.settings['active_hours_start']
        end_hour = self.settings['active_hours_end']
        
        return start_hour <= current_hour < end_hour


class DMStickerManager:
    """贴纸管理器 - 支持多贴纸包，不重复"""
    
    def __init__(self):
        self.sticker_sets_file = os.path.join(Config.CONFIG_DIR, 'dm_sticker_sets.json')
        self.sticker_sets = []  # 贴纸包名称列表
        self.used_sticker_ids = set()  # 已使用的贴纸ID
        self.sticker_cache = {}  # 贴纸包缓存
        self.load_sticker_sets()
    
    def load_sticker_sets(self):
        """加载贴纸包列表"""
        try:
            if os.path.exists(self.sticker_sets_file):
                with open(self.sticker_sets_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.sticker_sets = data.get('sticker_sets', [])
                    logger.info(f'加载了 {len(self.sticker_sets)} 个贴纸包')
            else:
                # 默认添加 HotCherry 贴纸包
                self.sticker_sets = ['HotCherry']
                self.save_sticker_sets()
        except Exception as e:
            logger.error(f'加载贴纸包列表失败: {e}')
            self.sticker_sets = ['HotCherry']
    
    def save_sticker_sets(self):
        """保存贴纸包列表"""
        try:
            with open(self.sticker_sets_file, 'w', encoding='utf-8') as f:
                json.dump({'sticker_sets': self.sticker_sets}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f'保存贴纸包列表失败: {e}')
    
    def add_sticker_set(self, set_name: str) -> bool:
        """添加贴纸包"""
        if set_name not in self.sticker_sets:
            self.sticker_sets.append(set_name)
            self.save_sticker_sets()
            return True
        return False
    
    def remove_sticker_set(self, set_name: str) -> bool:
        """移除贴纸包"""
        if set_name in self.sticker_sets:
            self.sticker_sets.remove(set_name)
            self.save_sticker_sets()
            return True
        return False
    
    def get_all_sticker_sets(self) -> list:
        """获取所有贴纸包"""
        return self.sticker_sets.copy()
    
    async def get_sticker_set(self, client, set_name: str):
        """获取贴纸包（带缓存）"""
        if set_name not in self.sticker_cache:
            try:
                from telethon.tl.functions.messages import GetStickerSetRequest
                from telethon.tl.types import InputStickerSetShortName
                
                sticker_set = await client(GetStickerSetRequest(
                    stickerset=InputStickerSetShortName(short_name=set_name),
                    hash=0
                ))
                self.sticker_cache[set_name] = sticker_set
            except Exception as e:
                logger.error(f'获取贴纸包 {set_name} 失败: {e}')
                return None
        return self.sticker_cache.get(set_name)
    
    async def get_random_sticker(self, client):
        """从所有贴纸包中随机选择一个不重复的贴纸"""
        if not self.sticker_sets:
            logger.warning("没有配置贴纸包")
            return None
        
        # 打乱贴纸包顺序
        shuffled_sets = self.sticker_sets.copy()
        random.shuffle(shuffled_sets)
        
        for set_name in shuffled_sets:
            sticker_set = await self.get_sticker_set(client, set_name)
            if not sticker_set:
                continue
            
            # 获取未使用的贴纸
            available = [s for s in sticker_set.documents 
                        if s.id not in self.used_sticker_ids]
            
            if available:
                sticker = random.choice(available)
                self.used_sticker_ids.add(sticker.id)
                logger.info(f"🍒 选择贴纸: {set_name} / ID: {sticker.id}")
                return sticker
        
        # 所有贴纸都用完了，重置
        logger.info("🍒 所有贴纸已用完，重新开始")
        self.used_sticker_ids.clear()
        
        # 重新选择
        return await self.get_random_sticker(client)
    
    def reset_used_stickers(self):
        """重置已使用的贴纸"""
        self.used_sticker_ids.clear()
        logger.info("🍒 已重置贴纸使用记录")


# ===== FSM 状态 =====
class BotStates(StatesGroup):
    """Bot 状态机"""
    waiting_for_keywords = State()
    waiting_delete_keywords = State()
    # 账号登录流程
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    # 过滤设置
    waiting_for_cooldown = State()
    waiting_for_max_length = State()
    waiting_for_min_age = State()
    # 黑名单移除
    waiting_remove_blacklist_user = State()


class ExportStates(StatesGroup):
    """数据导出状态机"""
    waiting_time_range = State()      # 等待输入时间范围
    waiting_keyword_filter = State()  # 等待输入关键词筛选
    choosing_format = State()         # 选择导出格式


class DMStates(StatesGroup):
    """私信号池状态机"""
    waiting_for_session_zip = State()       # 等待上传 session ZIP
    waiting_for_text_template = State()     # 等待输入文本话术
    waiting_for_postbot_code = State()      # 等待输入PostBot代码
    waiting_for_postbot_image = State()     # 等待上传图片
    waiting_for_postbot_text = State()      # 等待输入图文内容
    waiting_for_postbot_buttons = State()   # 等待输入按钮
    waiting_for_channel_link = State()      # 等待输入频道链接


class SendConfigStates(StatesGroup):
    """发送频率配置状态机"""
    waiting_delay = State()        # 等待输入延迟间隔
    waiting_batch = State()        # 等待输入批次设置
    waiting_daily_limit = State()  # 等待输入每日上限
    waiting_active_hours = State() # 等待输入活跃时段


# ===== 内联按钮 =====
class Keyboards:
    """内联键盘"""
    
    @staticmethod
    def main_menu(accounts_count: int = 0, online_count: int = 0, keywords_count: int = 0, 
                 dm_available: int = 0, dm_total: int = 0) -> InlineKeyboardMarkup:
        """主菜单"""
        keyboard = [
            [
                InlineKeyboardButton(text="📱 监控账号", callback_data="menu_accounts"),
                InlineKeyboardButton(text="📝 关键词管理", callback_data="menu_keywords")
            ],
            [
                InlineKeyboardButton(text="💬 私信号池", callback_data="menu_dm_pool"),
                InlineKeyboardButton(text="📤 数据导出", callback_data="menu_export")
            ],
            [
                InlineKeyboardButton(text="⚙️ 过滤设置", callback_data="menu_filters"),
                InlineKeyboardButton(text="📊 运行状态", callback_data="menu_status")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def accounts_menu() -> InlineKeyboardMarkup:
        """账号管理菜单"""
        keyboard = [
            [
                InlineKeyboardButton(text="➕ 添加新账号", callback_data="accounts_add"),
                InlineKeyboardButton(text="📋 账号列表", callback_data="accounts_list")
            ],
            [
                InlineKeyboardButton(text="🔄 全部重连", callback_data="accounts_reconnect"),
                InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def account_detail(phone: str) -> InlineKeyboardMarkup:
        """账号详情菜单"""
        # 使用phone的hash作为callback_data的一部分，避免太长
        phone_hash = abs(hash(phone)) % 100000
        keyboard = [
            [
                InlineKeyboardButton(text="🔄 重新连接", callback_data=f"acc_reconnect_{phone_hash}"),
                InlineKeyboardButton(text="🚪 退出登录", callback_data=f"acc_logout_{phone_hash}")
            ],
            [
                InlineKeyboardButton(text="❌ 删除账号", callback_data=f"acc_delete_{phone_hash}"),
                InlineKeyboardButton(text="🔙 返回列表", callback_data="accounts_list")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def accounts_list_buttons(accounts: List[Dict]) -> InlineKeyboardMarkup:
        """账号列表按钮"""
        keyboard = []
        for acc in accounts:
            name = acc.get('name', '未知')
            username = acc.get('username', '无')
            status = '🟢' if acc.get('enabled', False) else '🔴'
            phone_hash = abs(hash(acc['phone'])) % 100000
            display_text = f"{status} {name} (@{username})"[:50]
            keyboard.append([
                InlineKeyboardButton(text=display_text, callback_data=f"acc_detail_{phone_hash}")
            ])
        keyboard.append([
            InlineKeyboardButton(text="🔙 返回", callback_data="menu_accounts")
        ])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def keywords_menu() -> InlineKeyboardMarkup:
        """关键词管理菜单"""
        keyboard = [
            [
                InlineKeyboardButton(text="➕ 添加关键词", callback_data="keywords_add"),
                InlineKeyboardButton(text="➖ 删除关键词", callback_data="keywords_delete")
            ],
            [
                InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def filters_menu(settings: Dict) -> InlineKeyboardMarkup:
        """过滤设置菜单"""
        cooldown = settings.get('cooldown_minutes', 5)
        max_len = settings.get('max_message_length', 100)
        min_age = settings.get('min_account_age_days', 7)
        no_username = '✅ 开启' if settings.get('filter_no_username', True) else '❌ 关闭'
        no_avatar = '✅ 开启' if settings.get('filter_no_avatar', False) else '❌ 关闭'
        
        keyboard = [
            [InlineKeyboardButton(text=f"🔢 冷却时间: {cooldown}分钟", callback_data="filter_cooldown")],
            [InlineKeyboardButton(text=f"📏 消息长度限制: {max_len}字", callback_data="filter_max_length")],
            [InlineKeyboardButton(text=f"📅 账号年龄: {min_age}天", callback_data="filter_min_age")],
            [InlineKeyboardButton(text=f"👤 无用户名过滤: {no_username}", callback_data="filter_no_username")],
            [InlineKeyboardButton(text=f"📝 无头像过滤: {no_avatar}", callback_data="filter_no_avatar")],
            [InlineKeyboardButton(text="🚫 黑名单管理", callback_data="menu_blacklist")],
            [InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def export_menu() -> InlineKeyboardMarkup:
        """数据导出菜单"""
        keyboard = [
            [InlineKeyboardButton(text="📅 按时间段导出", callback_data="export_by_time")],
            [InlineKeyboardButton(text="🔑 按关键词导出", callback_data="export_by_keyword")],
            [InlineKeyboardButton(text="📋 导出全部数据", callback_data="export_all")],
            [InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def export_format_menu() -> InlineKeyboardMarkup:
        """导出格式选择菜单"""
        keyboard = [
            [InlineKeyboardButton(text="👤 仅用户名 (TXT)", callback_data="format_username")],
            [InlineKeyboardButton(text="🆔 仅用户ID (TXT)", callback_data="format_userid")],
            [InlineKeyboardButton(text="📊 完整记录 (CSV)", callback_data="format_csv")],
            [InlineKeyboardButton(text="🔙 返回", callback_data="menu_export")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def cancel_export() -> InlineKeyboardMarkup:
        """取消导出按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 取消", callback_data="menu_export")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def back_to_main() -> InlineKeyboardMarkup:
        """返回主菜单按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def back_to_keywords() -> InlineKeyboardMarkup:
        """返回关键词管理按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 返回", callback_data="menu_keywords")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def back_to_accounts() -> InlineKeyboardMarkup:
        """返回账号管理按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 返回", callback_data="menu_accounts")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def message_action_buttons(chat_id: int, msg_id: int, user_id: int, username: str = None, chat_username: str = None) -> InlineKeyboardMarkup:
        """消息快捷操作按钮"""
        # 构建私信按钮
        if username:
            # 有 username - 使用 URL 按钮直接跳转
            dm_button = InlineKeyboardButton(
                text="💬 一键私信", 
                url=f"https://t.me/{username}"
            )
        else:
            # 无 username - 使用回调按钮
            dm_button = InlineKeyboardButton(
                text="💬 一键私信", 
                callback_data=f"dm_nousername_{user_id}"
            )
        
        # 构建直达消息按钮
        if chat_username:
            # 公开群组 - 使用 URL 按钮直接跳转
            msg_button = InlineKeyboardButton(
                text="🚀 直达消息",
                url=f"https://t.me/{chat_username}/{msg_id}"
            )
        else:
            # 私有群组 - 使用回调按钮
            msg_button = InlineKeyboardButton(
                text="🚀 直达消息",
                callback_data=f"msg_link_{chat_id}_{msg_id}"
            )
        
        keyboard = [
            [
                msg_button,
                dm_button
            ],
            [
                InlineKeyboardButton(text="🚫 屏蔽用户", callback_data=f"block_user_{user_id}"),
                InlineKeyboardButton(text="🚫 屏蔽此群", callback_data=f"block_chat_{chat_id}")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def blacklist_menu(users_count: int, chats_count: int) -> InlineKeyboardMarkup:
        """黑名单管理菜单"""
        keyboard = [
            [InlineKeyboardButton(text=f"👥 已屏蔽用户 ({users_count})", callback_data="blacklist_users")],
            [InlineKeyboardButton(text=f"💬 已屏蔽群组 ({chats_count})", callback_data="blacklist_chats")],
            [
                InlineKeyboardButton(text="🗑️ 清空用户黑名单", callback_data="blacklist_clear_users"),
                InlineKeyboardButton(text="🗑️ 清空群组黑名单", callback_data="blacklist_clear_chats")
            ],
            [InlineKeyboardButton(text="🔙 返回", callback_data="menu_filters")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def blacklist_users_list(page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
        """黑名单用户列表 - 分页导航"""
        keyboard = []
        
        # 分页导航按钮
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"bl_users_page_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="bl_users_page_info"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"bl_users_page_{page+1}"))
            keyboard.append(nav_buttons)
        
        # 移除用户按钮
        keyboard.append([
            InlineKeyboardButton(text="🗑️ 移除用户", callback_data="bl_remove_user_start")
        ])
        
        # 返回按钮
        keyboard.append([
            InlineKeyboardButton(text="🔙 返回", callback_data="menu_blacklist")
        ])
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def blacklist_chats_list(chats: List[Dict]) -> InlineKeyboardMarkup:
        """黑名单群组列表"""
        keyboard = []
        for chat in chats[:20]:  # 最多显示20个
            title = chat.get('title', '未知群组')
            keyboard.append([
                InlineKeyboardButton(text=f"❌ {title[:25]}", callback_data=f"unblock_chat_{chat['chat_id']}")
            ])
        keyboard.append([
            InlineKeyboardButton(text="🔙 返回", callback_data="menu_blacklist")
        ])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_pool_menu(enabled: bool, available_count: int, total_count: int, 
                     today_sent: int, today_success: int, today_failed: int) -> InlineKeyboardMarkup:
        """私信号池管理菜单"""
        status_text = "✅ 开启" if enabled else "❌ 关闭"
        keyboard = [
            [InlineKeyboardButton(text=f"🔄 开关: {status_text}", callback_data="dm_toggle")],
            [
                InlineKeyboardButton(text="🔌 连接私信号", callback_data="dm_connect_clients"),
                InlineKeyboardButton(text="📤 上传Session", callback_data="dm_upload_session")
            ],
            [
                InlineKeyboardButton(text="📋 账号列表", callback_data="dm_accounts_list"),
                InlineKeyboardButton(text="🔍 检查全部状态", callback_data="dm_check_all_status")
            ],
            [
                InlineKeyboardButton(text="📝 私信话术", callback_data="dm_templates"),
                InlineKeyboardButton(text="⏰ 发送设置", callback_data="dm_settings")
            ],
            [
                InlineKeyboardButton(text="📊 私信记录", callback_data="dm_records"),
                InlineKeyboardButton(text="🔙 返回主菜单", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_accounts_list_buttons(page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
        """私信号账号列表按钮（分页导航）"""
        keyboard = []
        
        # 分页导航按钮
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"dm_acc_page_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="dm_acc_page_info"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton(text="➡️ 下一页", callback_data=f"dm_acc_page_{page+1}"))
            keyboard.append(nav_buttons)
        
        keyboard.append([
            InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")
        ])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_templates_menu(template_count: int) -> InlineKeyboardMarkup:
        """私信话术管理菜单"""
        keyboard = [
            [InlineKeyboardButton(text="➕ 添加话术", callback_data="dm_template_add")],
            [InlineKeyboardButton(text="📋 话术列表", callback_data="dm_template_list")],
            [InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_template_types() -> InlineKeyboardMarkup:
        """话术类型选择"""
        keyboard = [
            [InlineKeyboardButton(text="📝 文本直发", callback_data="dm_tpl_type_text")],
            [InlineKeyboardButton(text="🖼️ 图文+按钮", callback_data="dm_tpl_type_postbot")],
            [InlineKeyboardButton(text="📢 频道转发", callback_data="dm_tpl_type_forward")],
            [InlineKeyboardButton(text="👻 隐藏来源转发", callback_data="dm_tpl_type_forward_hidden")],
            [InlineKeyboardButton(text="🔙 取消", callback_data="dm_templates")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_template_list_buttons(templates: List[Dict]) -> InlineKeyboardMarkup:
        """话术列表按钮"""
        keyboard = []
        type_emoji = {
            'text': '📝',
            'postbot': '🖼️',
            'forward': '📢',
            'forward_hidden': '👻'
        }
        for tpl in templates[:20]:
            tpl_type = tpl.get('type', 'text')
            emoji = type_emoji.get(tpl_type, '📝')
            tpl_id = tpl.get('id', 0)
            
            # 获取简短描述
            content = tpl.get('content', {})
            if tpl_type == 'text':
                desc = content.get('text', '')[:20]
            elif tpl_type == 'postbot':
                desc = "图文消息"
            elif tpl_type in ['forward', 'forward_hidden']:
                desc = content.get('channel_link', '')[:20]
            else:
                desc = "未知类型"
            
            keyboard.append([
                InlineKeyboardButton(text=f"{emoji} {desc}", callback_data=f"dm_tpl_detail_{tpl_id}")
            ])
        keyboard.append([
            InlineKeyboardButton(text="🔙 返回", callback_data="dm_templates")
        ])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_text_template_options(use_emoji: bool, use_timestamp: bool, use_synonym: bool) -> InlineKeyboardMarkup:
        """文本话术防风控设置"""
        emoji_text = "✅ 开启" if use_emoji else "❌ 关闭"
        timestamp_text = "✅ 开启" if use_timestamp else "❌ 关闭"
        synonym_text = "✅ 开启" if use_synonym else "❌ 关闭"
        
        keyboard = [
            [InlineKeyboardButton(text=f"随机Emoji: {emoji_text}", callback_data="dm_tpl_opt_emoji")],
            [InlineKeyboardButton(text=f"随机时间戳: {timestamp_text}", callback_data="dm_tpl_opt_timestamp")],
            [InlineKeyboardButton(text=f"同义词替换: {synonym_text}", callback_data="dm_tpl_opt_synonym")],
            [
                InlineKeyboardButton(text="💾 保存", callback_data="dm_tpl_save"),
                InlineKeyboardButton(text="🔙 取消", callback_data="dm_templates")
            ]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def back_to_dm_pool() -> InlineKeyboardMarkup:
        """返回私信号池按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_status_filter_menu() -> InlineKeyboardMarkup:
        """账号状态筛选菜单 - 导出后会删除账号"""
        keyboard = [
            [
                InlineKeyboardButton(text="✅ 导出并删除正常账号", callback_data="dm_export_normal"),
                InlineKeyboardButton(text="⚠️ 导出并删除受限账号", callback_data="dm_export_restricted")
            ],
            [
                InlineKeyboardButton(text="❌ 导出并删除失效账号", callback_data="dm_export_invalid"),
                InlineKeyboardButton(text="📋 导出并删除全部账号", callback_data="dm_export_all")
            ],
            [InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def dm_send_config_menu(settings: Dict) -> InlineKeyboardMarkup:
        """发送频率配置菜单"""
        keyboard = [
            [InlineKeyboardButton(
                text=f"⏱️ 修改延迟间隔 ({settings['delay_min']}-{settings['delay_max']}秒)",
                callback_data="dm_config_delay"
            )],
            [InlineKeyboardButton(
                text=f"📦 修改批次设置 ({settings['batch_size']}条)",
                callback_data="dm_config_batch"
            )],
            [InlineKeyboardButton(
                text=f"📊 修改每日上限 ({settings['daily_limit']}条/账号)",
                callback_data="dm_config_daily_limit"
            )],
            [InlineKeyboardButton(
                text=f"🕐 修改活跃时段 ({settings['active_hours_start']}:00-{settings['active_hours_end']}:00)",
                callback_data="dm_config_active_hours"
            )],
            [InlineKeyboardButton(text="🍒 贴纸打招呼", callback_data="dm_sticker_settings")],
            [InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    @staticmethod
    def cancel_config() -> InlineKeyboardMarkup:
        """取消配置按钮"""
        keyboard = [[InlineKeyboardButton(text="🔙 取消", callback_data="dm_settings")]]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ===== JTBot 主类 =====
class JTBot:
    """JTBot 主类 - 多账号监控"""
    
    def __init__(self):
        Config.validate()
        
        # 管理器
        self.keyword_manager = KeywordManager(Config.KEYWORDS_FILE)
        self.account_manager = AccountManager(Config.ACCOUNTS_FILE)
        self.filter_manager = FilterManager(Config.FILTER_SETTINGS_FILE)
        self.record_manager = RecordManager(Config.RECORDS_FILE)
        self.blacklist_manager = BlacklistManager(Config.BLACKLIST_FILE)
        
        # DM 私信号池管理器
        self.dm_account_manager = DMAccountManager(Config.DM_ACCOUNTS_FILE)
        self.dm_template_manager = DMTemplateManager(Config.DM_TEMPLATES_FILE)
        self.dm_record_manager = DMRecordManager(Config.DM_RECORDS_FILE, Config.DM_SENT_USERS_FILE)
        self.dm_settings_manager = DMSettingsManager(Config.DM_SETTINGS_FILE)
        self.dm_sticker_manager = DMStickerManager()  # 贴纸管理器
        
        # 确保目录存在
        os.makedirs(Config.CONFIG_DIR, exist_ok=True)
        os.makedirs(Config.DM_SESSIONS_DIR, exist_ok=True)
        os.makedirs(Config.EXPORTS_DIR, exist_ok=True)
        
        # DM 客户端
        self.dm_clients: Dict[str, TelegramClient] = {}  # phone -> client
        
        # Bot (管理界面)
        self.bot = Bot(token=Config.BOT_TOKEN)
        self.dp = Dispatcher(storage=MemoryStorage())
        
        # 代理配置
        self.proxy = ProxyParser.load_proxy_from_file(Config.PROXY_FILE)
        
        # 多账号客户端
        self.clients: Dict[str, TelegramClient] = {}  # phone -> client
        self.client_tasks: Dict[str, asyncio.Task] = {}  # phone -> task
        
        # 防重复转发缓存: {user_id}_{keyword} -> last_trigger_time
        cooldown_seconds = self.filter_manager.get_setting('cooldown_minutes') * 60
        self.cooldown_cache = TTLCache(maxsize=10000, ttl=cooldown_seconds)
        
        # 消息去重缓存: {chat_id}_{msg_id} -> timestamp (5分钟TTL)
        self.processed_messages = TTLCache(maxsize=10000, ttl=300)
        
        # 用于账号登录的临时存储
        self.login_data: Dict[int, Dict] = {}  # user_id -> {phone, client}
        
        # 导出相关临时数据
        self.export_data: Dict[int, Dict] = {}  # user_id -> export context
        
        # DM 相关临时数据
        self.dm_template_temp: Dict[int, Dict] = {}  # user_id -> template temp data
        
        # 账号状态映射 (phone_hash -> phone)
        self.phone_hash_map: Dict[int, str] = {}
        self.dm_phone_hash_map: Dict[int, str] = {}  # DM账号的hash映射
        
        # 统计信息
        self.stats = {
            'messages_received': 0,
            'keywords_matched': 0,
            'filtered_count': 0,
            'start_time': datetime.now()
        }
        
        # 注册处理器
        self.register_handlers()
    
    async def _safe_edit_message(self, message, text: str, reply_markup=None):
        """安全地编辑消息，避免"message is not modified"错误"""
        try:
            if reply_markup:
                await message.edit_text(text, reply_markup=reply_markup)
            else:
                await message.edit_text(text)
        except Exception as e:
            # 如果消息内容相同，忽略错误
            if "message is not modified" not in str(e):
                logger.error(f"编辑消息失败: {e}")
                raise
    
    def _get_phone_by_hash(self, phone_hash: int) -> Optional[str]:
        """通过hash获取phone"""
        return self.phone_hash_map.get(phone_hash)
    
    def _update_phone_hash_map(self):
        """更新phone hash映射"""
        self.phone_hash_map.clear()
        for acc in self.account_manager.get_all_accounts():
            phone_hash = abs(hash(acc['phone'])) % 100000
            self.phone_hash_map[phone_hash] = acc['phone']
    
    def _parse_time_range(self, text: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        解析时间范围: 01-08-00:00|01-10-23:59
        返回: (start_datetime, end_datetime)
        """
        parts = text.split('|')
        if len(parts) != 2:
            return None, None
        
        current_year = datetime.now().year
        
        try:
            # 解析 MM-DD-HH:MM 格式
            start_str = parts[0].strip()
            end_str = parts[1].strip()
            
            start_dt = datetime.strptime(f"{current_year}-{start_str}", "%Y-%m-%d-%H:%M")
            end_dt = datetime.strptime(f"{current_year}-{end_str}", "%Y-%m-%d-%H:%M")
            
            return start_dt, end_dt
        except:
            return None, None
    
    async def _export_data(self, records: List[Dict], format_type: str, filter_info: str) -> str:
        """
        导出数据
        format_type: 'username' | 'userid' | 'csv'
        返回: 文件路径
        """
        # 确保exports目录存在
        os.makedirs(Config.EXPORTS_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format_type == 'username':
            filename = os.path.join(Config.EXPORTS_DIR, f"users_username_{timestamp}.txt")
            with open(filename, 'w', encoding='utf-8') as f:
                usernames = set()
                for r in records:
                    if r.get('username'):
                        usernames.add(f"@{r['username']}")
                f.write('\n'.join(sorted(usernames)))
        
        elif format_type == 'userid':
            filename = os.path.join(Config.EXPORTS_DIR, f"users_id_{timestamp}.txt")
            with open(filename, 'w', encoding='utf-8') as f:
                user_ids = set(str(r['user_id']) for r in records)
                f.write('\n'.join(sorted(user_ids)))
        
        elif format_type == 'csv':
            filename = os.path.join(Config.EXPORTS_DIR, f"records_{timestamp}.csv")
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['用户ID', '用户名', '昵称', '来源群组', '触发关键词', '触发时间', '消息内容'])
                for r in records:
                    writer.writerow([
                        r['user_id'],
                        r.get('username', ''),
                        r.get('name', ''),
                        r.get('chat_title', ''),
                        r.get('keyword', ''),
                        r.get('time', ''),
                        r.get('message', '')
                    ])
        
        return filename
    
    
    def register_handlers(self):
        """注册 Bot 处理器"""
        
        @self.dp.message(Command('start'))
        async def cmd_start(message: Message):
            if message.from_user.id != Config.ADMIN_USER_ID:
                await message.answer("⛔ 无权限访问")
                return
            
            accounts = self.account_manager.get_all_accounts()
            online_count = sum(1 for acc in accounts if acc['phone'] in self.clients and self.clients[acc['phone']].is_connected())
            keywords_count = len(self.keyword_manager.keywords)
            
            # DM 统计
            dm_accounts = self.dm_account_manager.get_all_accounts()
            dm_available = len([acc for acc in dm_accounts if acc.get('status') == 'active'])
            dm_abnormal = len(dm_accounts) - dm_available
            
            text = f"🤖 JTBot 关键词监控机器人\n\n"
            text += f"📱 监控账号: {online_count}在线\n"
            text += f"🔑 关键词: {keywords_count}个\n"
            text += f"💬 私信号池: {dm_available}可用 / {dm_abnormal}异常"
            
            await message.answer(
                text,
                reply_markup=Keyboards.main_menu(len(accounts), online_count, keywords_count, dm_available, len(dm_accounts))
            )
        
        @self.dp.callback_query(F.data == "menu_main")
        async def menu_main(callback: CallbackQuery):
            await callback.answer()
            
            accounts = self.account_manager.get_all_accounts()
            online_count = sum(1 for acc in accounts if acc['phone'] in self.clients and self.clients[acc['phone']].is_connected())
            keywords_count = len(self.keyword_manager.keywords)
            
            # DM 统计
            dm_accounts = self.dm_account_manager.get_all_accounts()
            dm_available = len([acc for acc in dm_accounts if acc.get('status') == 'active'])
            dm_abnormal = len(dm_accounts) - dm_available
            
            text = f"🤖 JTBot 关键词监控机器人\n\n"
            text += f"📱 监控账号: {online_count}在线\n"
            text += f"🔑 关键词: {keywords_count}个\n"
            text += f"💬 私信号池: {dm_available}可用 / {dm_abnormal}异常"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.main_menu(len(accounts), online_count, keywords_count, dm_available, len(dm_accounts))
            )
        
        @self.dp.callback_query(F.data == "menu_accounts")
        async def menu_accounts(callback: CallbackQuery):
            await callback.answer()
            
            accounts = self.account_manager.get_all_accounts()
            online_count = sum(1 for acc in accounts if acc['phone'] in self.clients and self.clients[acc['phone']].is_connected())
            
            text = f"📱 监控账号管理\n\n"
            text += f"已登录账号: {len(accounts)}/{self.account_manager.max_accounts}\n"
            text += f"在线: {online_count} | 离线: {len(accounts) - online_count}"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.accounts_menu()
            )
        
        @self.dp.callback_query(F.data == "accounts_list")
        async def accounts_list(callback: CallbackQuery):
            await callback.answer()
            
            accounts = self.account_manager.get_all_accounts()
            if not accounts:
                await callback.message.edit_text(
                    "❌ 暂无监控账号\n\n点击 [➕ 添加新账号] 开始添加",
                    reply_markup=Keyboards.back_to_accounts()
                )
            else:
                self._update_phone_hash_map()
                text = f"📋 账号列表 ({len(accounts)}个):\n\n"
                for i, acc in enumerate(accounts, 1):
                    name = acc.get('name', '未知')
                    username = acc.get('username', '无')
                    phone = acc['phone']
                    is_online = phone in self.clients and self.clients[phone].is_connected()
                    status = '🟢 在线' if is_online else '🔴 离线'
                    text += f"{i}. {name} (@{username}) {status}\n"
                
                await callback.message.edit_text(
                    text,
                    reply_markup=Keyboards.accounts_list_buttons(accounts)
                )

        
        @self.dp.callback_query(F.data == "accounts_add")
        async def accounts_add(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            if len(self.account_manager.get_all_accounts()) >= self.account_manager.max_accounts:
                await callback.message.edit_text(
                    f"❌ 已达到最大账号数量限制 ({self.account_manager.max_accounts}个)",
                    reply_markup=Keyboards.back_to_accounts()
                )
                return
            
            await callback.message.edit_text(
                "请输入监控账号的手机号\n\n格式: +8613800138000",
                reply_markup=Keyboards.back_to_accounts()
            )
            await state.set_state(BotStates.waiting_for_phone)
            await callback.answer()
        
        @self.dp.message(BotStates.waiting_for_phone)
        async def receive_phone(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            phone = message.text.strip()
            
            if not re.match(r'^\+\d{10,15}$', phone):
                await message.answer(
                    "❌ 手机号格式不正确\n\n请使用国际格式，例如: +8613800138000",
                    reply_markup=Keyboards.back_to_accounts()
                )
                return
            
            if self.account_manager.get_account(phone):
                await message.answer(
                    "❌ 该账号已存在",
                    reply_markup=Keyboards.accounts_menu()
                )
                await state.clear()
                return
            
            session_name = f"session_{phone.replace('+', '')}"
            session_path = os.path.join(Config.SESSIONS_DIR, session_name)
            
            client = TelegramClient(
                session_path,
                Config.API_ID,
                Config.API_HASH,
                proxy=self.proxy
            )
            
            try:
                await client.connect()
                await client.send_code_request(phone)
                
                self.login_data[message.from_user.id] = {
                    'phone': phone,
                    'client': client,
                    'session_file': session_name
                }
                
                await message.answer(
                    "✅ 验证码已发送！\n\n请输入您在 Telegram 收到的验证码",
                    reply_markup=Keyboards.back_to_accounts()
                )
                await state.set_state(BotStates.waiting_for_code)
                
            except Exception as e:
                logger.error(f"发送验证码失败: {e}")
                await message.answer(
                    f"❌ 发送验证码失败: {str(e)}",
                    reply_markup=Keyboards.accounts_menu()
                )
                await state.clear()
                if client.is_connected():
                    await client.disconnect()
        
        @self.dp.message(BotStates.waiting_for_code)
        async def receive_code(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            code = message.text.strip()
            login_info = self.login_data.get(message.from_user.id)
            
            if not login_info:
                await message.answer("❌ 登录会话已过期，请重新开始")
                await state.clear()
                return
            
            client = login_info['client']
            phone = login_info['phone']
            
            try:
                await client.sign_in(phone, code)
                me = await client.get_me()
                
                success = self.account_manager.add_account(
                    phone=phone,
                    session_file=login_info['session_file'],
                    name=me.first_name or '未知',
                    username=me.username or '',
                    user_id=me.id
                )
                
                if success:
                    self.clients[phone] = client
                    
                    @client.on(events.NewMessage())
                    async def handle_msg(event):
                        await self.handle_new_message(event, phone)
                    
                    await message.answer(
                        f"✅ 登录成功！\n\n"
                        f"账号: {me.first_name} (@{me.username or '无'})\n"
                        f"ID: {me.id}\n\n"
                        f"监控已自动开始",
                        reply_markup=Keyboards.accounts_menu()
                    )
                else:
                    await message.answer("❌ 保存账号失败")
                
                del self.login_data[message.from_user.id]
                await state.clear()
                
            except SessionPasswordNeededError:
                await message.answer(
                    "🔐 账号已启用两步验证\n\n请输入您的两步验证密码",
                    reply_markup=Keyboards.back_to_accounts()
                )
                await state.set_state(BotStates.waiting_for_password)
                
            except PhoneCodeInvalidError:
                await message.answer(
                    "❌ 验证码错误\n\n请重新输入验证码",
                    reply_markup=Keyboards.back_to_accounts()
                )
            except Exception as e:
                logger.error(f"登录失败: {e}")
                await message.answer(
                    f"❌ 登录失败: {str(e)}",
                    reply_markup=Keyboards.accounts_menu()
                )
                await state.clear()
                del self.login_data[message.from_user.id]
                if client.is_connected():
                    await client.disconnect()
        
        @self.dp.message(BotStates.waiting_for_password)
        async def receive_password(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            password = message.text.strip()
            login_info = self.login_data.get(message.from_user.id)
            
            if not login_info:
                await message.answer("❌ 登录会话已过期")
                await state.clear()
                return
            
            client = login_info['client']
            phone = login_info['phone']
            
            try:
                await client.sign_in(password=password)
                me = await client.get_me()
                
                success = self.account_manager.add_account(
                    phone=phone,
                    session_file=login_info['session_file'],
                    name=me.first_name or '未知',
                    username=me.username or '',
                    user_id=me.id
                )
                
                if success:
                    self.clients[phone] = client
                    
                    @client.on(events.NewMessage())
                    async def handle_msg(event):
                        await self.handle_new_message(event, phone)
                    
                    await message.answer(
                        f"✅ 登录成功！\n\n"
                        f"账号: {me.first_name} (@{me.username or '无'})\n"
                        f"监控已自动开始",
                        reply_markup=Keyboards.accounts_menu()
                    )
                else:
                    await message.answer("❌ 保存账号失败")
                
                del self.login_data[message.from_user.id]
                await state.clear()
                
            except Exception as e:
                logger.error(f"两步验证登录失败: {e}")
                await message.answer(
                    f"❌ 密码错误或登录失败: {str(e)}",
                    reply_markup=Keyboards.accounts_menu()
                )
                await state.clear()
                del self.login_data[message.from_user.id]
                if client.is_connected():
                    await client.disconnect()
        
        @self.dp.callback_query(F.data.startswith("acc_detail_"))
        async def account_detail(callback: CallbackQuery):
            phone_hash = int(callback.data.replace("acc_detail_", ""))
            phone = self._get_phone_by_hash(phone_hash)
            
            if not phone:
                await callback.answer("❌ 账号不存在")
                return
            
            acc = self.account_manager.get_account(phone)
            if not acc:
                await callback.answer("❌ 账号不存在")
                return
            
            is_online = phone in self.clients and self.clients[phone].is_connected()
            status = '🟢 在线' if is_online else '🔴 离线'
            
            text = f"📱 账号详情\n\n"
            text += f"姓名: {acc['name']}\n"
            text += f"用户名: @{acc['username'] or '无'}\n"
            text += f"手机号: {phone}\n"
            text += f"状态: {status}\n"
            text += f"添加时间: {acc['added_at'][:10]}"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.account_detail(phone)
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data.startswith("acc_delete_"))
        async def account_delete(callback: CallbackQuery):
            phone_hash = int(callback.data.replace("acc_delete_", ""))
            phone = self._get_phone_by_hash(phone_hash)
            
            if not phone:
                await callback.answer("❌ 账号不存在")
                return
            
            if phone in self.clients:
                try:
                    await self.clients[phone].disconnect()
                except:
                    pass
                del self.clients[phone]
            
            if self.account_manager.remove_account(phone):
                await callback.answer("✅ 账号已删除")
                await accounts_list(callback)
            else:
                await callback.answer("❌ 删除失败")
        
        @self.dp.callback_query(F.data == "menu_keywords")
        async def menu_keywords(callback: CallbackQuery):
            await callback.answer()
            
            keywords = self.keyword_manager.get_keywords()
            if keywords:
                keyword_str = "|".join(keywords)
                text = f"📝 关键词列表 ({len(keywords)}个):\n\n{keyword_str}"
            else:
                text = "📝 关键词列表为空"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.keywords_menu()
            )
        
        @self.dp.callback_query(F.data == "keywords_add")
        async def keywords_add(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            await callback.message.edit_text(
                "请发送要添加的关键词，多个关键词用 | 分隔\n\n"
                "⚠️ 关键词长度限制: 最多10个字符\n"
                "示例: 求购|想买|收一个",
                reply_markup=Keyboards.back_to_keywords()
            )
            await state.set_state(BotStates.waiting_for_keywords)
        
        @self.dp.message(BotStates.waiting_for_keywords)
        async def receive_keywords(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            keywords = [k.strip() for k in message.text.split('|')]
            total = len(keywords)
            added = self.keyword_manager.add_keywords(keywords)
            
            response = f"✅ 成功添加 {added} 个关键词"
            if added < total:
                response += f"\n⚠️ {total - added} 个关键词因长度超过10字符被忽略"
            
            await message.answer(
                response,
                reply_markup=Keyboards.keywords_menu()
            )
            await state.clear()
        
        @self.dp.callback_query(F.data == "keywords_delete")
        async def keywords_delete(callback: CallbackQuery, state: FSMContext):
            keywords = self.keyword_manager.get_keywords()
            if not keywords:
                await callback.answer("❌ 没有关键词可删除", show_alert=True)
                return
            
            keyword_str = "|".join(keywords)
            text = f"当前关键词:\n{keyword_str}\n\n请直接发送要删除的关键词\n多个关键词用 | 分隔\n示例: 求购|想买"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 取消", callback_data="menu_keywords")]
            ])
            
            await callback.message.edit_text(text, reply_markup=keyboard)
            await state.set_state(BotStates.waiting_delete_keywords)
            await callback.answer()
        
        @self.dp.message(BotStates.waiting_delete_keywords)
        async def process_delete_keywords(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            input_text = message.text.strip()
            keywords_to_delete = [kw.strip() for kw in input_text.split("|") if kw.strip()]
            
            keywords = self.keyword_manager.get_keywords()
            deleted = []
            not_found = []
            
            for kw in keywords_to_delete:
                if kw in keywords:
                    if self.keyword_manager.remove_keyword(kw):
                        deleted.append(kw)
                else:
                    not_found.append(kw)
            
            await state.clear()
            
            result_text = ""
            if deleted:
                result_text += f"✅ 成功删除 {len(deleted)} 个关键词: {', '.join(deleted)}\n"
            if not_found:
                result_text += f"❌ 以下关键词不存在: {', '.join(not_found)}"
            
            if not result_text:
                result_text = "❌ 未找到要删除的关键词"
            
            await message.answer(result_text, reply_markup=Keyboards.keywords_menu())
        
        @self.dp.callback_query(F.data == "menu_filters")
        async def menu_filters(callback: CallbackQuery):
            await callback.answer()
            
            settings = self.filter_manager.settings
            text = "⚙️ 过滤设置\n\n"
            text += "点击下方按钮修改设置："
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.filters_menu(settings)
            )
        
        @self.dp.callback_query(F.data == "filter_no_username")
        async def toggle_no_username(callback: CallbackQuery):
            current = self.filter_manager.get_setting('filter_no_username')
            self.filter_manager.update_setting('filter_no_username', not current)
            await menu_filters(callback)
        
        @self.dp.callback_query(F.data == "filter_no_avatar")
        async def toggle_no_avatar(callback: CallbackQuery):
            current = self.filter_manager.get_setting('filter_no_avatar')
            self.filter_manager.update_setting('filter_no_avatar', not current)
            await menu_filters(callback)
        
        @self.dp.callback_query(F.data == "filter_cooldown")
        async def set_cooldown(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            await callback.message.edit_text(
                "请输入冷却时间（分钟）\n\n范围: 1-60分钟",
                reply_markup=Keyboards.back_to_main()
            )
            await state.set_state(BotStates.waiting_for_cooldown)
        
        @self.dp.message(BotStates.waiting_for_cooldown)
        async def receive_cooldown(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                minutes = int(message.text.strip())
                if 1 <= minutes <= 60:
                    self.filter_manager.update_setting('cooldown_minutes', minutes)
                    self.cooldown_cache = TTLCache(maxsize=10000, ttl=minutes * 60)
                    await message.answer(
                        f"✅ 冷却时间已设置为 {minutes} 分钟",
                        reply_markup=Keyboards.filters_menu(self.filter_manager.settings)
                    )
                else:
                    await message.answer("❌ 请输入1-60之间的数字")
            except ValueError:
                await message.answer("❌ 请输入有效的数字")
            
            await state.clear()
        
        @self.dp.callback_query(F.data == "filter_max_length")
        async def set_max_length(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            await callback.message.edit_text(
                "请输入消息长度限制（字符数）\n\n"
                "超过此长度的消息将不会被转发\n"
                "范围: 10-1000字符",
                reply_markup=Keyboards.back_to_main()
            )
            await state.set_state(BotStates.waiting_for_max_length)
        
        @self.dp.message(BotStates.waiting_for_max_length)
        async def receive_max_length(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                max_len = int(message.text.strip())
                if 10 <= max_len <= 1000:
                    self.filter_manager.update_setting('max_message_length', max_len)
                    await message.answer(
                        f"✅ 消息长度限制已设置为 {max_len} 字符",
                        reply_markup=Keyboards.filters_menu(self.filter_manager.settings)
                    )
                else:
                    await message.answer("❌ 请输入10-1000之间的数字")
            except ValueError:
                await message.answer("❌ 请输入有效的数字")
            
            await state.clear()
        
        @self.dp.callback_query(F.data == "filter_min_age")
        async def set_min_age(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            await callback.message.edit_text(
                "请输入最小账号年龄（天数）\n\n范围: 0-365天\n设置为0则不限制",
                reply_markup=Keyboards.back_to_main()
            )
            await state.set_state(BotStates.waiting_for_min_age)
        
        @self.dp.message(BotStates.waiting_for_min_age)
        async def receive_min_age(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                days = int(message.text.strip())
                if 0 <= days <= 365:
                    self.filter_manager.update_setting('min_account_age_days', days)
                    await message.answer(
                        f"✅ 最小账号年龄已设置为 {days} 天",
                        reply_markup=Keyboards.filters_menu(self.filter_manager.settings)
                    )
                else:
                    await message.answer("❌ 请输入0-365之间的数字")
            except ValueError:
                await message.answer("❌ 请输入有效的数字")
            
            await state.clear()
        
        @self.dp.callback_query(F.data == "menu_export")
        async def menu_export(callback: CallbackQuery):
            await callback.answer()
            
            record_count = len(self.record_manager.records)
            text = f"📤 数据导出\n\n"
            text += f"当前记录数: {record_count}\n\n"
            text += "请选择导出方式："
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.export_menu()
            )
        
        @self.dp.callback_query(F.data == "export_by_time")
        async def export_by_time(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            await callback.message.edit_text(
                "📅 按时间段导出\n\n"
                "请输入时间范围，格式：\n"
                "开始时间|结束时间\n\n"
                "示例: 01-05-00:00|01-10-23:59\n"
                "(表示1月5日0点 到 1月10日23点59分)",
                reply_markup=Keyboards.cancel_export()
            )
            await state.set_state(ExportStates.waiting_time_range)
        
        @self.dp.message(ExportStates.waiting_time_range)
        async def receive_time_range(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            start_time, end_time = self._parse_time_range(message.text.strip())
            
            if not start_time or not end_time:
                await message.answer(
                    "❌ 时间格式错误\n\n请使用格式: MM-DD-HH:MM|MM-DD-HH:MM\n示例: 01-05-00:00|01-10-23:59",
                    reply_markup=Keyboards.cancel_export()
                )
                return
            
            # 保存过滤条件
            self.export_data[message.from_user.id] = {
                'start_time': start_time,
                'end_time': end_time,
                'filter_type': 'time'
            }
            
            # 显示格式选择
            filtered_count = len(self.record_manager.filter_records(start_time=start_time, end_time=end_time))
            await message.answer(
                f"✅ 已选择时间段\n\n"
                f"从 {start_time.strftime('%m-%d %H:%M')} 到 {end_time.strftime('%m-%d %H:%M')}\n"
                f"共 {filtered_count} 条记录\n\n"
                f"请选择导出格式：",
                reply_markup=Keyboards.export_format_menu()
            )
            await state.set_state(ExportStates.choosing_format)
        
        @self.dp.callback_query(F.data == "export_by_keyword")
        async def export_by_keyword(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            current_keywords = self.keyword_manager.get_keywords()
            keywords_str = "|".join(current_keywords) if current_keywords else "无"
            
            await callback.message.edit_text(
                f"🔑 按关键词导出\n\n"
                f"当前关键词:\n{keywords_str}\n\n"
                f"请输入要导出的关键词\n"
                f"多个关键词用 | 分隔\n"
                f"示例: 飞机号|求购",
                reply_markup=Keyboards.cancel_export()
            )
            await state.set_state(ExportStates.waiting_keyword_filter)
        
        @self.dp.message(ExportStates.waiting_keyword_filter)
        async def receive_keyword_filter(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            keywords = [k.strip() for k in message.text.split('|') if k.strip()]
            
            if not keywords:
                await message.answer(
                    "❌ 关键词不能为空",
                    reply_markup=Keyboards.cancel_export()
                )
                return
            
            # 保存过滤条件
            self.export_data[message.from_user.id] = {
                'keywords': keywords,
                'filter_type': 'keyword'
            }
            
            # 显示格式选择
            filtered_count = len(self.record_manager.filter_records(keywords=keywords))
            await message.answer(
                f"✅ 已选择关键词\n\n"
                f"关键词: {', '.join(keywords)}\n"
                f"共 {filtered_count} 条记录\n\n"
                f"请选择导出格式：",
                reply_markup=Keyboards.export_format_menu()
            )
            await state.set_state(ExportStates.choosing_format)
        
        @self.dp.callback_query(F.data == "export_all")
        async def export_all(callback: CallbackQuery, state: FSMContext):
            await callback.answer()
            
            # 保存过滤条件
            self.export_data[callback.from_user.id] = {
                'filter_type': 'all'
            }
            
            record_count = len(self.record_manager.records)
            await callback.message.edit_text(
                f"📋 导出全部数据\n\n"
                f"共 {record_count} 条记录\n\n"
                f"请选择导出格式：",
                reply_markup=Keyboards.export_format_menu()
            )
            await state.set_state(ExportStates.choosing_format)
        
        @self.dp.callback_query(F.data.startswith("format_"))
        async def export_format_selected(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            format_type = callback.data.replace("format_", "")
            export_ctx = self.export_data.get(callback.from_user.id, {})
            
            if not export_ctx:
                await callback.answer("❌ 导出上下文已过期", show_alert=True)
                await state.clear()
                return
            
            try:
                await callback.message.edit_text("⏳ 正在生成导出文件...")
                
                # 根据过滤条件获取记录
                filter_type = export_ctx.get('filter_type')
                if filter_type == 'time':
                    records = self.record_manager.filter_records(
                        start_time=export_ctx.get('start_time'),
                        end_time=export_ctx.get('end_time')
                    )
                    filter_info = f"时间段: {export_ctx['start_time'].strftime('%m-%d %H:%M')} 到 {export_ctx['end_time'].strftime('%m-%d %H:%M')}"
                elif filter_type == 'keyword':
                    records = self.record_manager.filter_records(keywords=export_ctx.get('keywords'))
                    filter_info = f"关键词: {', '.join(export_ctx['keywords'])}"
                else:  # all
                    records = self.record_manager.records
                    filter_info = "全部数据"
                
                if not records:
                    await callback.message.edit_text(
                        "❌ 没有符合条件的记录",
                        reply_markup=Keyboards.export_menu()
                    )
                    await state.clear()
                    del self.export_data[callback.from_user.id]
                    return
                
                # 导出数据
                filename = await self._export_data(records, format_type, filter_info)
                
                # 发送文件
                with open(filename, 'rb') as f:
                    file_data = f.read()
                    file = BufferedInputFile(file_data, filename=os.path.basename(filename))
                    
                    caption = f"✅ 导出完成\n\n"
                    caption += f"过滤条件: {filter_info}\n"
                    caption += f"记录数: {len(records)}"
                    
                    await callback.message.answer_document(file, caption=caption)
                
                await callback.message.edit_text(
                    "✅ 导出成功！",
                    reply_markup=Keyboards.export_menu()
                )
                
            except Exception as e:
                logger.error(f"导出失败: {e}", exc_info=True)
                await callback.message.edit_text(
                    f"❌ 导出失败: {str(e)}",
                    reply_markup=Keyboards.export_menu()
                )
            finally:
                await state.clear()
                if callback.from_user.id in self.export_data:
                    del self.export_data[callback.from_user.id]
            
            await callback.answer()
        
        # Legacy export handlers for backward compatibility (removed)
        # These are replaced by the new export flow
        
        @self.dp.callback_query(F.data == "menu_status")
        async def menu_status(callback: CallbackQuery):
            await callback.answer()
            
            uptime = datetime.now() - self.stats['start_time']
            hours = int(uptime.total_seconds() // 3600)
            minutes = int((uptime.total_seconds() % 3600) // 60)
            
            accounts = self.account_manager.get_all_accounts()
            online_count = sum(1 for acc in accounts if acc['phone'] in self.clients and self.clients[acc['phone']].is_connected())
            
            text = f"📊 运行状态\n\n"
            text += f"⏱ 运行时间: {hours}小时{minutes}分钟\n"
            text += f"📱 监控账号: {len(accounts)}个 ({online_count}在线)\n"
            text += f"🔑 关键词: {len(self.keyword_manager.keywords)}个\n"
            text += f"📨 接收消息: {self.stats['messages_received']}\n"
            text += f"🔔 关键词匹配: {self.stats['keywords_matched']}\n"
            text += f"🚫 过滤拦截: {self.stats['filtered_count']}\n"
            text += f"📝 记录数: {len(self.record_manager.records)}"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.back_to_main()
            )
        
        @self.dp.callback_query(F.data == "menu_help")
        async def menu_help(callback: CallbackQuery):
            text = (
                "❓ 帮助信息\n\n"
                "📱 账号管理:\n"
                "• 点击 [➕ 添加新账号] 进行交互式登录\n"
                "• 支持多个监控账号同时工作\n"
                "• 每个账号独立监控已加入的群组\n\n"
                "📝 关键词管理:\n"
                "• 添加/删除关键词（长度≤10字符）\n"
                "• 消息包含关键词时自动转发\n\n"
                "⚙️ 过滤设置:\n"
                "• 冷却时间: 防止重复转发\n"
                "• 消息长度: 超长消息不转发\n"
                "• 用户过滤: 过滤特定类型用户\n"
                "• 黑名单管理: 屏蔽用户和群组\n\n"
                "📤 数据导出:\n"
                "• 导出触发用户列表\n"
                "• 导出完整触发记录\n\n"
                "🚀 消息快捷按钮:\n"
                "• 直达消息: 跳转到原始消息\n"
                "• 一键私信: 快速打开私聊\n"
                "• 屏蔽用户/群组: 加入黑名单"
            )
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.back_to_main()
            )
        
        # ===== 黑名单管理回调 =====
        @self.dp.callback_query(F.data == "menu_blacklist")
        async def menu_blacklist(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            users = self.blacklist_manager.get_users()
            chats = self.blacklist_manager.get_chats()
            
            text = "⚙️ 设置 → 🚫 黑名单管理\n\n"
            text += f"已屏蔽用户: {len(users)}\n"
            text += f"已屏蔽群组: {len(chats)}"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.blacklist_menu(len(users), len(chats))
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "blacklist_users")
        async def blacklist_users(callback: CallbackQuery, state: FSMContext):
            """显示黑名单用户列表 - 第1页"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 清除状态（如果从移除流程返回）
            await state.clear()
            
            users = self.blacklist_manager.get_users()
            if not users:
                await callback.message.edit_text(
                    "✅ 用户黑名单为空",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 返回", callback_data="menu_blacklist")
                    ]])
                )
            else:
                await show_blacklist_users_page(callback, page=1)
            await callback.answer()
        
        async def show_blacklist_users_page(callback: CallbackQuery, page: int = 1):
            """显示黑名单用户列表的指定页"""
            users = self.blacklist_manager.get_users()
            total_users = len(users)
            
            # 处理空列表情况
            if total_users == 0:
                await callback.message.edit_text(
                    "✅ 用户黑名单为空",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 返回", callback_data="menu_blacklist")
                    ]])
                )
                return
            
            per_page = 20
            total_pages = (total_users + per_page - 1) // per_page  # 向上取整
            
            # 确保页码有效
            page = max(1, min(page, total_pages))
            
            # 计算当前页的用户范围
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total_users)
            page_users = users[start_idx:end_idx]
            
            # 构建消息文本
            text = f"👥 用户黑名单 (第{page}/{total_pages}页，共{total_users}个)\n\n"
            text += "点击ID可复制\n\n"
            
            for user in page_users:
                user_id = user['user_id']
                username = user.get('username', '')
                if username and username != '无':
                    text += f"`{user_id}` @{username}\n"
                else:
                    text += f"`{user_id}`\n"
            
            # 显示消息
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.blacklist_users_list(page, total_pages),
                parse_mode="Markdown"
            )
        
        @self.dp.callback_query(F.data.startswith("bl_users_page_"))
        async def blacklist_users_page(callback: CallbackQuery):
            """处理黑名单用户列表分页"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 提取页码
            if callback.data == "bl_users_page_info":
                await callback.answer()
                return
            
            try:
                page = int(callback.data.replace("bl_users_page_", ""))
                await show_blacklist_users_page(callback, page)
                await callback.answer()
            except (ValueError, IndexError):
                await callback.answer("❌ 页码错误")
        
        @self.dp.callback_query(F.data == "bl_remove_user_start")
        async def bl_remove_user_start(callback: CallbackQuery, state: FSMContext):
            """开始移除黑名单用户流程"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            users = self.blacklist_manager.get_users()
            total_users = len(users)
            
            text = "🗑️ 移除黑名单用户\n\n"
            text += "请发送要移除的用户ID\n"
            text += "支持多个ID，用空格、逗号或换行分隔\n\n"
            text += "示例: 7804079885 8533238613"
            
            await callback.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 取消", callback_data="blacklist_users")
                ]])
            )
            
            # 设置状态
            await state.set_state(BotStates.waiting_remove_blacklist_user)
            await callback.answer()
        
        @self.dp.message(BotStates.waiting_remove_blacklist_user)
        async def process_remove_blacklist_user(message: Message, state: FSMContext):
            """处理移除黑名单用户的消息"""
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            # 检查消息类型
            if not message.text:
                await message.answer(
                    "❌ 请发送文本消息（用户ID）",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 返回黑名单", callback_data="blacklist_users")
                    ]])
                )
                return
            
            # 解析用户输入的ID列表（支持空格、换行和逗号分隔）
            text = message.text.strip()
            user_ids_str = re.split(r'[\s,]+', text)
            
            removed_ids = []
            not_found_ids = []
            invalid_ids = []
            
            for user_id_str in user_ids_str:
                user_id_str = user_id_str.strip()
                if not user_id_str:
                    continue
                
                try:
                    user_id = int(user_id_str)
                    if self.blacklist_manager.remove_user(user_id):
                        removed_ids.append(user_id)
                    else:
                        not_found_ids.append(user_id)
                except ValueError:
                    invalid_ids.append(user_id_str)
            
            # 构建结果消息
            users = self.blacklist_manager.get_users()
            total_users = len(users)
            
            result_text = ""
            if removed_ids:
                result_text += f"✅ 已移除 {len(removed_ids)} 个用户:\n"
                result_text += ", ".join(str(uid) for uid in removed_ids)
                result_text += "\n\n"
            
            if not_found_ids:
                result_text += f"⚠️ 未在黑名单中找到 {len(not_found_ids)} 个ID:\n"
                result_text += ", ".join(str(uid) for uid in not_found_ids)
                result_text += "\n\n"
            
            if invalid_ids:
                result_text += f"❌ 无效的ID格式 ({len(invalid_ids)}个):\n"
                result_text += ", ".join(invalid_ids)
                result_text += "\n\n"
            
            if not removed_ids and not not_found_ids and not invalid_ids:
                result_text = "❌ 未识别到有效的用户ID\n\n"
            
            result_text += "继续发送ID移除，或点击返回"
            
            await message.answer(
                result_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text=f"🔙 返回黑名单 ({total_users}人)", callback_data="blacklist_users")
                ]])
            )
            # 保持状态，允许继续移除
        
        @self.dp.callback_query(F.data == "blacklist_chats")
        async def blacklist_chats(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            chats = self.blacklist_manager.get_chats()
            if not chats:
                await callback.message.edit_text(
                    "✅ 群组黑名单为空",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 返回", callback_data="menu_blacklist")
                    ]])
                )
            else:
                text = f"💬 已屏蔽群组 ({len(chats)}):\n\n"
                text += "点击群组移除黑名单："
                await callback.message.edit_text(
                    text,
                    reply_markup=Keyboards.blacklist_chats_list(chats)
                )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "blacklist_clear_users")
        async def blacklist_clear_users(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            self.blacklist_manager.clear_users()
            await callback.answer("✅ 用户黑名单已清空")
            await menu_blacklist(callback)
        
        @self.dp.callback_query(F.data == "blacklist_clear_chats")
        async def blacklist_clear_chats(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            self.blacklist_manager.clear_chats()
            await callback.answer("✅ 群组黑名单已清空")
            await menu_blacklist(callback)
        
        @self.dp.callback_query(F.data.startswith("unblock_user_"))
        async def unblock_user(callback: CallbackQuery, state: FSMContext):
            """旧版移除用户回调 - 保留兼容性"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                user_id = int(callback.data.replace("unblock_user_", ""))
                if self.blacklist_manager.remove_user(user_id):
                    await callback.answer("✅ 已移除用户")
                    await blacklist_users(callback, state)
                else:
                    await callback.answer("❌ 移除失败")
            except ValueError:
                await callback.answer("❌ 无效的用户ID")
        
        @self.dp.callback_query(F.data.startswith("unblock_chat_"))
        async def unblock_chat(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                chat_id = int(callback.data.replace("unblock_chat_", ""))
                if self.blacklist_manager.remove_chat(chat_id):
                    await callback.answer("✅ 已移除群组")
                    await blacklist_chats(callback)
                else:
                    await callback.answer("❌ 移除失败")
            except ValueError:
                await callback.answer("❌ 无效的群组ID")
        
        # ===== 消息快捷操作回调 =====
        @self.dp.callback_query(F.data.startswith("msg_link_"))
        async def msg_link(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                parts = callback.data.replace("msg_link_", "").split("_")
                if len(parts) < 2:
                    await callback.answer("❌ 无效的消息数据", show_alert=True)
                    return
                
                chat_id = int(parts[0])
                msg_id = int(parts[1])
                
                # 尝试生成消息链接
                # 对于负数chat_id（超级群组），需要特殊处理
                if chat_id < 0:
                    chat_id_str = str(chat_id)
                    # 超级群组：去掉-100前缀（例如：-1001234567890 -> 1234567890）
                    if chat_id_str.startswith("-100"):
                        group_id = chat_id_str[4:]  # 去掉 "-100"
                        link = f"https://t.me/c/{group_id}/{msg_id}"
                    else:
                        link = "私有群组，无法生成链接"
                else:
                    link = "私有群组，无法生成链接"
                
                await callback.answer(f"📎 消息链接:\n{link}", show_alert=True)
            except (ValueError, IndexError) as e:
                logger.error(f"解析消息数据失败: {e}")
                await callback.answer("❌ 数据格式错误", show_alert=True)
            except Exception as e:
                logger.error(f"生成消息链接失败: {e}")
                await callback.answer("❌ 生成链接失败", show_alert=True)
        
        @self.dp.callback_query(F.data.startswith("dm_user_"))
        async def dm_user(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                user_id = int(callback.data.replace("dm_user_", ""))
                
                # 生成私信链接 - 使用tg://协议，适用于所有情况
                link = f"tg://user?id={user_id}"
                
                await callback.answer(f"💬 私信链接:\n{link}", show_alert=True)
            except Exception as e:
                logger.error(f"生成私信链接失败: {e}")
                await callback.answer("❌ 生成链接失败", show_alert=True)
        
        @self.dp.callback_query(F.data.startswith("dm_nousername_"))
        async def handle_dm_no_username(callback: CallbackQuery):
            """处理无username用户的私信按钮点击"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                user_id = callback.data.split("_")[2]
                await callback.answer(
                    f"该用户无用户名，请手动搜索用户ID: {user_id}",
                    show_alert=True
                )
            except Exception as e:
                logger.error(f"处理无username私信失败: {e}")
                await callback.answer("❌ 处理失败", show_alert=True)
        
        @self.dp.callback_query(F.data.startswith("block_user_"))
        async def block_user(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                user_id = int(callback.data.replace("block_user_", ""))
                
                if self.blacklist_manager.add_user(user_id):
                    await callback.answer("✅ 已将用户加入黑名单", show_alert=True)
                    logger.info(f"用户 {user_id} 已加入黑名单")
                else:
                    await callback.answer("⚠️ 用户已在黑名单中", show_alert=True)
            except Exception as e:
                logger.error(f"屏蔽用户失败: {e}")
                await callback.answer("❌ 屏蔽失败", show_alert=True)
        
        @self.dp.callback_query(F.data.startswith("block_chat_"))
        async def block_chat(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            try:
                chat_id = int(callback.data.replace("block_chat_", ""))
                
                if self.blacklist_manager.add_chat(chat_id):
                    await callback.answer("✅ 已将群组加入黑名单", show_alert=True)
                    logger.info(f"群组 {chat_id} 已加入黑名单")
                else:
                    await callback.answer("⚠️ 群组已在黑名单中", show_alert=True)
            except Exception as e:
                logger.error(f"屏蔽群组失败: {e}")
                await callback.answer("❌ 屏蔽失败", show_alert=True)
        
        # ===== 私信号池管理回调 =====
        @self.dp.callback_query(F.data == "menu_dm_pool")
        async def menu_dm_pool(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            enabled = self.dm_settings_manager.get_setting('enabled')
            dm_accounts = self.dm_account_manager.get_all_accounts()
            available_count = len([acc for acc in dm_accounts if acc.get('status') == 'active'])
            total_count = len(dm_accounts)
            abnormal_count = total_count - available_count
            
            # 获取今日统计
            stats = self.dm_record_manager.get_stats()
            
            text = f"💬 私信号池管理\n\n"
            text += f"状态: {'✅ 已开启' if enabled else '❌ 已关闭'}\n"
            text += f"可用: {available_count} | 异常: {abnormal_count} | 总计: {total_count}\n"
            text += f"今日私信: 发送 {stats['total_sent']} | 成功 {stats['success']} | 失败 {stats['failed']}"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.dm_pool_menu(
                    enabled, available_count, total_count,
                    stats['total_sent'], stats['success'], stats['failed']
                )
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_toggle")
        async def dm_toggle(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            current = self.dm_settings_manager.get_setting('enabled')
            self.dm_settings_manager.update_setting('enabled', not current)
            
            await callback.answer(f"✅ 私信号池已{'开启' if not current else '关闭'}")
            await menu_dm_pool(callback)
        
        @self.dp.callback_query(F.data == "dm_connect_clients")
        async def dm_connect_clients(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 立即回应callback，避免超时
            await callback.answer("🔌 开始连接...")
            
            status_msg = await callback.message.edit_text("🔌 正在连接私信号...")
            
            # 获取所有DM账号
            accounts = self.dm_account_manager.get_all_accounts()
            if not accounts:
                await status_msg.edit_text(
                    "❌ 没有可连接的私信号\n\n请先上传Session文件",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
                return
            
            # 并发连接函数
            async def connect_dm_client(acc):
                phone = acc['phone']
                
                # 如果已经连接，跳过
                if phone in self.dm_clients and self.dm_clients[phone].is_connected():
                    return {'success': True, 'phone': phone, 'client': None, 'already_connected': True}
                
                session_file = acc['session_file']
                session_path = os.path.join(Config.DM_SESSIONS_DIR, session_file.replace('.session', ''))
                
                try:
                    # 尝试代理连接
                    connection_type = 'unknown'
                    client = None
                    
                    if self.proxy:
                        try:
                            client = TelegramClient(
                                session_path,
                                Config.API_ID,
                                Config.API_HASH,
                                proxy=self.proxy
                            )
                            await asyncio.wait_for(client.connect(), timeout=10)
                            connection_type = 'proxy'
                        except asyncio.TimeoutError:
                            logger.info(f"代理连接超时，尝试本地连接: {phone}")
                            if client:
                                await client.disconnect()
                            client = None
                    
                    if not client:
                        # 本地连接
                        client = TelegramClient(
                            session_path,
                            Config.API_ID,
                            Config.API_HASH
                        )
                        await client.connect()
                        connection_type = 'local'
                    
                    if not await client.is_user_authorized():
                        logger.warning(f"私信号 {phone} session 已过期")
                        self.dm_account_manager.update_account_status(phone, 'failed', False)
                        await client.disconnect()
                        return {'success': False, 'phone': phone, 'client': None}
                    
                    me = await client.get_me()
                    logger.info(f"✅ 私信号 {me.first_name} ({phone}) 已连接 [{connection_type}]")
                    
                    # 更新连接状态
                    self.dm_account_manager.update_account_status(phone, acc.get('status', 'active'), acc.get('can_send_dm', True))
                    
                    return {
                        'success': True,
                        'phone': phone,
                        'client': client,
                        'already_connected': False
                    }
                    
                except Exception as e:
                    logger.error(f"连接私信号 {phone} 失败: {e}")
                    self.dm_account_manager.update_account_status(phone, 'failed', False)
                    return {'success': False, 'phone': phone, 'client': None}
            
            # 并发连接，每批10个
            batch_size = 10
            connected = 0
            failed = 0
            total = len(accounts)
            
            for i in range(0, total, batch_size):
                batch = accounts[i:i + batch_size]
                
                # 并发执行连接
                tasks = [connect_dm_client(acc) for acc in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 统计结果并保存客户端
                for result in results:
                    if isinstance(result, dict):
                        if result['success']:
                            connected += 1
                            # 保存新连接的客户端
                            if result['client'] and not result.get('already_connected'):
                                self.dm_clients[result['phone']] = result['client']
                        else:
                            failed += 1
                    else:
                        # 异常情况
                        failed += 1
            
            # 显示结果
            result_text = f"✅ 连接完成！\n\n"
            result_text += f"✅ 成功: {connected} 个\n"
            result_text += f"❌ 失败: {failed} 个"
            
            await status_msg.edit_text(
                result_text,
                reply_markup=Keyboards.back_to_dm_pool()
            )
        
        @self.dp.callback_query(F.data == "dm_upload_session")
        async def dm_upload_session(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            await callback.message.edit_text(
                "📤 上传 Session 文件\n\n"
                "🤖 请发送 session 文件:\n"
                "• 支持格式: .session 或 .zip（含多个session）\n"
                "• ZIP文件会自动解压并检测所有session\n\n"
                "⚠️ 上传后将自动检测账号状态",
                reply_markup=Keyboards.back_to_dm_pool()
            )
            await state.set_state(DMStates.waiting_for_session_zip)
            await callback.answer()
        
        @self.dp.message(DMStates.waiting_for_session_zip)
        async def receive_session_file(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            if not message.document:
                await message.answer(
                    "❌ 请发送文件",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
                return
            
            file = message.document
            file_name = file.file_name
            
            # 检查文件类型
            if not (file_name.endswith('.zip') or file_name.endswith('.session')):
                await message.answer(
                    "❌ 不支持的文件格式\n\n仅支持 .session 或 .zip 文件",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
                return
            
            try:
                # 确保 dm_sessions 目录存在
                os.makedirs(Config.DM_SESSIONS_DIR, exist_ok=True)
                
                # 下载文件
                status_msg = await message.answer("⏳ 正在下载文件...")
                
                file_path = os.path.join('/tmp', file_name)
                await self.bot.download(file, destination=file_path)
                
                session_files = []
                
                if file_name.endswith('.zip'):
                    # 解压 ZIP
                    await status_msg.edit_text("📦 正在解压...")
                    
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        # 查找所有 .session 文件
                        session_names = [name for name in zip_ref.namelist() if name.endswith('.session')]
                        
                        if not session_names:
                            await status_msg.edit_text(
                                "❌ ZIP文件中没有找到 .session 文件",
                                reply_markup=Keyboards.back_to_dm_pool()
                            )
                            await state.clear()
                            return
                        
                        await status_msg.edit_text(f"📦 发现 {len(session_names)} 个 session 文件")
                        
                        # 解压所有文件到 dm_sessions 目录（保留原始文件，跳过journal）
                        for file_in_zip in zip_ref.namelist():
                            # 只提取文件名（不包含路径）
                            base_name = os.path.basename(file_in_zip)
                            if not base_name:  # 跳过目录
                                continue
                            
                            # 跳过 .session-journal 文件
                            if base_name.endswith('.session-journal'):
                                continue
                            
                            target_path = os.path.join(Config.DM_SESSIONS_DIR, base_name)
                            
                            with zip_ref.open(file_in_zip) as source, open(target_path, 'wb') as target:
                                target.write(source.read())
                            
                            # 只记录 .session 文件用于后续检测
                            if base_name.endswith('.session'):
                                session_files.append(base_name)
                else:
                    # 单个 .session 文件
                    target_path = os.path.join(Config.DM_SESSIONS_DIR, file_name)
                    os.rename(file_path, target_path)
                    session_files.append(file_name)
                
                # 检测所有账号状态（并发处理）
                await status_msg.edit_text("🔍 正在检测账号状态...")
                
                imported_count = 0
                failed_count = 0
                
                # 计时器用于计算预计时间
                start_time = time.time()
                last_update = start_time
                
                total = len(session_files)
                checked = 0
                
                # 并发检查函数
                async def check_and_import_session(session_file):
                    session_path = os.path.join(Config.DM_SESSIONS_DIR, session_file.replace('.session', ''))
                    
                    try:
                        # 尝试连接（先尝试代理，超时后本地）
                        connection_type = 'unknown'
                        client = None
                        
                        if self.proxy:
                            try:
                                client = TelegramClient(
                                    session_path,
                                    Config.API_ID,
                                    Config.API_HASH,
                                    proxy=self.proxy
                                )
                                await asyncio.wait_for(client.connect(), timeout=10)
                                connection_type = 'proxy'
                            except asyncio.TimeoutError:
                                logger.info(f"代理连接超时，尝试本地连接: {session_file}")
                                if client:
                                    await client.disconnect()
                                client = None
                        
                        if not client:
                            # 本地连接
                            client = TelegramClient(
                                session_path,
                                Config.API_ID,
                                Config.API_HASH
                            )
                            await client.connect()
                            connection_type = 'local'
                        
                        if not await client.is_user_authorized():
                            logger.warning(f"Session未授权: {session_file}")
                            await client.disconnect()
                            return {'success': False, 'client': None}
                        
                        # 获取用户信息
                        me = await client.get_me()
                        
                        # 检测账号状态（通过@SpamBot）
                        status, can_send_dm = await self.dm_account_manager.check_account_status(client)
                        
                        # 保存账号信息
                        phone = me.phone if me.phone else f"user_{me.id}"
                        self.dm_account_manager.add_account(
                            phone=phone,
                            session_file=session_file,
                            name=me.first_name or '未知',
                            username=me.username or '',
                            user_id=me.id,
                            status=status,
                            connection_type=connection_type
                        )
                        
                        logger.info(f"✅ 导入成功: {me.first_name} ({phone}) - {status}")
                        
                        return {
                            'success': True,
                            'phone': phone,
                            'client': client
                        }
                        
                    except Exception as e:
                        logger.error(f"导入session失败 {session_file}: {e}")
                        if client and client.is_connected():
                            await client.disconnect()
                        return {'success': False, 'client': None}
                
                # 并发检查，每批10个
                batch_size = 10
                
                for i in range(0, total, batch_size):
                    batch = session_files[i:i + batch_size]
                    batch_end = min(i + batch_size, total)
                    
                    # 并发执行检查
                    tasks = [check_and_import_session(sf) for sf in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # 统计结果并保存客户端
                    for result in results:
                        if isinstance(result, dict):
                            if result['success']:
                                imported_count += 1
                                # 保存客户端
                                if result['client']:
                                    self.dm_clients[result['phone']] = result['client']
                            else:
                                failed_count += 1
                        else:
                            # 异常情况
                            failed_count += 1
                        checked += 1
                    
                    # 每批更新一次进度（每5秒或完成时）
                    current_time = time.time()
                    if current_time - last_update >= 5 or checked == total:
                        # 计算预计剩余时间
                        elapsed_time = current_time - start_time
                        if checked > 0:
                            avg_time_per_account = elapsed_time / checked
                            remaining_accounts = total - checked
                            estimated_seconds = int(avg_time_per_account * remaining_accounts)
                            
                            if estimated_seconds >= 60:
                                estimated_time_str = f"{estimated_seconds // 60}分钟"
                            else:
                                estimated_time_str = f"{estimated_seconds}秒"
                        else:
                            estimated_time_str = "计算中..."
                        
                        # 更新进度显示
                        progress_text = f"🔍 正在检测账号状态 ({checked}/{total})...\n\n"
                        progress_text += f"✅ 可用: {imported_count}\n"
                        progress_text += f"❌ 异常: {failed_count}\n\n"
                        
                        if checked < total:
                            progress_text += f"⏳ 预计剩余时间: {estimated_time_str}"
                        
                        try:
                            await status_msg.edit_text(progress_text)
                            last_update = current_time
                        except Exception:
                            pass  # 忽略编辑失败
                
                # 显示结果
                result_text = f"✅ 导入完成！\n\n"
                result_text += f"✅ 可用: {imported_count} 个\n"
                result_text += f"❌ 异常: {failed_count} 个"
                
                await status_msg.edit_text(
                    result_text,
                    reply_markup=Keyboards.back_to_dm_pool()
                )
                
                # 清理临时文件
                if os.path.exists(file_path):
                    os.remove(file_path)
                
            except Exception as e:
                logger.error(f"处理session文件失败: {e}", exc_info=True)
                await message.answer(
                    f"❌ 处理失败: {str(e)}",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
            
            await state.clear()
        
        @self.dp.callback_query(F.data == "dm_accounts_list")
        async def dm_accounts_list(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 默认显示第1页
            await show_dm_accounts_page(callback, page=1)
        
        @self.dp.callback_query(F.data.startswith("dm_acc_page_"))
        async def dm_accounts_page(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            page_data = callback.data.replace("dm_acc_page_", "")
            if page_data == "info":
                await callback.answer()
                return
            
            try:
                page = int(page_data)
                await show_dm_accounts_page(callback, page)
            except ValueError:
                await callback.answer("❌ 无效的页码")
        
        async def show_dm_accounts_page(callback: CallbackQuery, page: int):
            """显示私信号列表的指定页"""
            accounts = self.dm_account_manager.get_all_accounts()
            if not accounts:
                await callback.message.edit_text(
                    "❌ 暂无私信号\n\n点击 [📤 上传Session] 开始添加",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
                await callback.answer()
                return
            
            # 分页设置
            per_page = 20  # 减少每页数量，因为每行信息更长了
            total_pages = (len(accounts) + per_page - 1) // per_page
            page = max(1, min(page, total_pages))
            
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, len(accounts))
            page_accounts = accounts[start_idx:end_idx]
            
            # 今日日期
            today = datetime.now().date().isoformat()
            
            # 状态文本映射
            status_text_map = {
                'active': '正常',
                'restricted': '受限',
                'spam': '受限',
                'banned': '封禁',
                'frozen': '冻结',
                'failed': '失败',
                'unknown': '未知'
            }
            
            # 生成显示文本
            text = f"📋 私信号列表 (第{page}/{total_pages}页，共{len(accounts)}个):\n\n"
            
            for i, acc in enumerate(page_accounts, start=start_idx + 1):
                phone = acc.get('phone', '未知')
                username = acc.get('username', '')
                status = acc.get('status', 'unknown')
                
                # 获取今日发送数量
                last_sent_date = acc.get('last_sent_date', '')
                if last_sent_date == today:
                    daily_sent = acc.get('daily_sent', 0)
                else:
                    daily_sent = 0
                
                # 状态emoji和文字
                status_emoji = self.dm_account_manager.get_status_emoji(status)
                status_name = status_text_map.get(status, '未知')
                
                # 用户名部分（无用户名则不显示）
                username_part = f"@{username}" if username else ""
                
                # 格式: 序号. 状态emoji 手机号 | @用户名 | 状态文字 | 今日:N条
                if username_part:
                    text += f"{i}. {status_emoji} {phone} | {username_part} | {status_name} | 今日:{daily_sent}条\n"
                else:
                    text += f"{i}. {status_emoji} {phone} | {status_name} | 今日:{daily_sent}条\n"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.dm_accounts_list_buttons(page, total_pages)
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_check_all_status")
        async def dm_check_all_status(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            accounts = self.dm_account_manager.get_all_accounts()
            if not accounts:
                await callback.answer("❌ 没有账号可检查", show_alert=True)
                return
            
            # 立即回应callback，避免超时
            await callback.answer("🔍 开始检查...")
            
            status_msg = await callback.message.edit_text("🔍 正在检查账号状态...")
            
            # 状态统计
            status_counts = {
                'active': 0,
                'restricted': 0,
                'spam': 0,
                'banned': 0,
                'frozen': 0,
                'failed': 0
            }
            
            # 计时器
            start_time = time.time()
            last_update = start_time
            
            # 并发检查函数
            async def check_single_account(acc):
                phone = acc['phone']
                try:
                    client = self.dm_clients.get(phone)
                    if not client or not client.is_connected():
                        self.dm_account_manager.update_account_status(phone, 'failed', False)
                        return 'failed'
                    
                    # 检测状态
                    status, can_send_dm = await self.dm_account_manager.check_account_status(client)
                    self.dm_account_manager.update_account_status(phone, status, can_send_dm)
                    return status
                    
                except Exception as e:
                    logger.error(f"检查账号状态失败 {phone}: {e}")
                    self.dm_account_manager.update_account_status(phone, 'failed', False)
                    return 'failed'
            
            # 并发检查，每批10个
            batch_size = 10
            total = len(accounts)
            checked = 0
            
            for i in range(0, total, batch_size):
                batch = accounts[i:i + batch_size]
                batch_end = min(i + batch_size, total)
                
                # 并发执行检查
                tasks = [check_single_account(acc) for acc in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 统计结果
                for result in results:
                    if isinstance(result, str):
                        status_counts[result] = status_counts.get(result, 0) + 1
                    checked += 1
                
                # 每5秒更新一次进度
                current_time = time.time()
                if current_time - last_update >= 5 or checked == total:
                    # 计算预计剩余时间
                    elapsed_time = current_time - start_time
                    if checked > 0:
                        avg_time_per_account = elapsed_time / checked
                        remaining_accounts = total - checked
                        estimated_seconds = int(avg_time_per_account * remaining_accounts)
                        
                        if estimated_seconds >= 60:
                            estimated_time_str = f"{estimated_seconds // 60}分钟"
                        else:
                            estimated_time_str = f"{estimated_seconds}秒"
                    else:
                        estimated_time_str = "计算中..."
                    
                    # 更新进度显示
                    progress_text = f"🔍 正在检测账号状态 ({checked}/{total})...\n\n"
                    progress_text += f"✅ 无限制: {status_counts['active']}\n"
                    progress_text += f"⚠️ 临时限制: {status_counts['restricted']}\n"
                    progress_text += f"📵 垃圾邮件: {status_counts['spam']}\n"
                    progress_text += f"🚫 封禁账号: {status_counts['banned']}\n"
                    progress_text += f"❄️ 冻结账号: {status_counts['frozen']}\n"
                    progress_text += f"🔌 连接失败: {status_counts['failed']}\n\n"
                    
                    if checked < total:
                        progress_text += f"⏳ 预计剩余时间: {estimated_time_str}"
                    
                    try:
                        await status_msg.edit_text(progress_text)
                        last_update = current_time
                    except Exception:
                        pass  # 忽略编辑失败（可能因为内容相同）
            
            # 最终结果
            result_text = f"✅ 检测完成！\n\n"
            result_text += f"总计: {total} 个账号\n\n"
            result_text += f"✅ 无限制: {status_counts['active']}\n"
            result_text += f"⚠️ 临时限制: {status_counts['restricted']}\n"
            result_text += f"📵 垃圾邮件: {status_counts['spam']}\n"
            result_text += f"🚫 封禁账号: {status_counts['banned']}\n"
            result_text += f"❄️ 冻结账号: {status_counts['frozen']}\n"
            result_text += f"🔌 连接失败: {status_counts['failed']}\n\n"
            result_text += f"⚠️ 提示: 导出后账号将从服务器删除"
            
            await status_msg.edit_text(
                result_text,
                reply_markup=Keyboards.dm_status_filter_menu()
            )
        
        @self.dp.callback_query(F.data == "dm_templates")
        async def dm_templates(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            templates = self.dm_template_manager.get_all_templates()
            
            text = f"📝 私信话术管理\n\n"
            if templates:
                text += f"已配置话术 ({len(templates)}条):\n\n"
                type_names = {
                    'text': '📝 文本直发',
                    'postbot': '🖼️ 图文+按钮',
                    'forward': '📢 频道转发',
                    'forward_hidden': '👻 隐藏转发'
                }
                for tpl in templates[:5]:  # 显示前5个
                    tpl_type = tpl.get('type', 'text')
                    type_name = type_names.get(tpl_type, '未知')
                    text += f"{tpl.get('id')}. {type_name}\n"
                
                if len(templates) > 5:
                    text += f"\n... 还有 {len(templates) - 5} 个话术"
            else:
                text += "暂无话术模板"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.dm_templates_menu(len(templates))
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_template_add")
        async def dm_template_add(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            await callback.message.edit_text(
                "➕ 添加话术\n\n请选择发送形式:",
                reply_markup=Keyboards.dm_template_types()
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_template_list")
        async def dm_template_list(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            templates = self.dm_template_manager.get_all_templates()
            if not templates:
                await callback.message.edit_text(
                    "❌ 暂无话术模板",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
            else:
                await callback.message.edit_text(
                    f"📋 话术列表 ({len(templates)}个):\n\n点击查看详情",
                    reply_markup=Keyboards.dm_template_list_buttons(templates)
                )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_tpl_type_text")
        async def dm_tpl_type_text(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 初始化临时数据
            self.dm_template_temp[callback.from_user.id] = {
                'type': 'text',
                'use_emoji': True,
                'use_timestamp': True,
                'use_synonym': False
            }
            
            await callback.message.edit_text(
                "📝 文本话术设置\n\n"
                "请发送话术内容，支持变体语法:\n"
                "示例: {你好|您好}，{看到|注意到}你在群里的消息\n\n"
                "发送后可设置防风控选项",
                reply_markup=Keyboards.back_to_dm_pool()
            )
            await state.set_state(DMStates.waiting_for_text_template)
            await callback.answer()
        
        @self.dp.message(DMStates.waiting_for_text_template)
        async def receive_text_template(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            text = message.text.strip()
            if not text:
                await message.answer("❌ 话术内容不能为空")
                return
            
            # 保存到临时数据
            temp_data = self.dm_template_temp.get(message.from_user.id, {})
            temp_data['text'] = text
            self.dm_template_temp[message.from_user.id] = temp_data
            
            # 显示防风控选项
            use_emoji = temp_data.get('use_emoji', True)
            use_timestamp = temp_data.get('use_timestamp', True)
            use_synonym = temp_data.get('use_synonym', False)
            
            await message.answer(
                f"📝 话术内容:\n{text}\n\n"
                "防风控选项:",
                reply_markup=Keyboards.dm_text_template_options(use_emoji, use_timestamp, use_synonym)
            )
            await state.clear()
        
        @self.dp.callback_query(F.data.startswith("dm_tpl_opt_"))
        async def dm_tpl_option_toggle(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            temp_data = self.dm_template_temp.get(callback.from_user.id, {})
            
            option = callback.data.replace("dm_tpl_opt_", "")
            if option == 'emoji':
                temp_data['use_emoji'] = not temp_data.get('use_emoji', True)
            elif option == 'timestamp':
                temp_data['use_timestamp'] = not temp_data.get('use_timestamp', True)
            elif option == 'synonym':
                temp_data['use_synonym'] = not temp_data.get('use_synonym', False)
            
            self.dm_template_temp[callback.from_user.id] = temp_data
            
            text = f"📝 话术内容:\n{temp_data.get('text', '')}\n\n防风控选项:"
            
            await callback.message.edit_text(
                text,
                reply_markup=Keyboards.dm_text_template_options(
                    temp_data.get('use_emoji', True),
                    temp_data.get('use_timestamp', True),
                    temp_data.get('use_synonym', False)
                )
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_tpl_save")
        async def dm_tpl_save(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            temp_data = self.dm_template_temp.get(callback.from_user.id, {})
            
            if 'text' not in temp_data:
                await callback.answer("❌ 没有话术内容", show_alert=True)
                return
            
            # 保存模板
            template_id = self.dm_template_manager.add_template(
                template_type='text',
                content={
                    'text': temp_data['text'],
                    'use_emoji': temp_data.get('use_emoji', True),
                    'use_timestamp': temp_data.get('use_timestamp', True),
                    'use_synonym': temp_data.get('use_synonym', False)
                }
            )
            
            # 清理临时数据
            if callback.from_user.id in self.dm_template_temp:
                del self.dm_template_temp[callback.from_user.id]
            
            await callback.answer("✅ 话术已保存")
            await dm_templates(callback)
        
        @self.dp.callback_query(F.data == "dm_tpl_type_postbot")
        async def dm_tpl_type_postbot(callback: CallbackQuery, state: FSMContext):
            """PostBot 图文+按钮类型处理"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            await callback.message.edit_text(
                "🖼️ 图文+按钮 (PostBot格式)\n\n"
                "请先在 @PostBot 中配置好图文消息\n"
                "然后发送 PostBot 生成的代码\n\n"
                "示例: ABC123",
                reply_markup=Keyboards.back_to_dm_pool()
            )
            await state.set_state(DMStates.waiting_for_postbot_code)
            await callback.answer()
        
        @self.dp.message(DMStates.waiting_for_postbot_code)
        async def receive_postbot_code(message: Message, state: FSMContext):
            """接收 PostBot 代码"""
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            code = message.text.strip()
            if not code:
                await message.answer("❌ PostBot 代码不能为空")
                return
            
            # 保存模板
            template_id = self.dm_template_manager.add_template(
                template_type='postbot',
                content={
                    'code': code
                }
            )
            
            await message.answer(
                f"✅ PostBot 话术已保存\n\n"
                f"代码: {code}\n"
                f"模板ID: {template_id}"
            )
            await state.clear()
            
            # 返回话术管理菜单
            templates = self.dm_template_manager.get_all_templates()
            text = f"📝 私信话术管理\n\n"
            if templates:
                text += f"已配置话术 ({len(templates)}条):\n\n"
                type_names = {
                    'text': '📝 文本直发',
                    'postbot': '🖼️ 图文+按钮',
                    'forward': '📢 频道转发',
                    'forward_hidden': '👻 隐藏转发'
                }
                for tpl in templates[:5]:
                    tpl_type = tpl.get('type', 'text')
                    type_name = type_names.get(tpl_type, '未知')
                    text += f"{tpl.get('id')}. {type_name}\n"
                
                if len(templates) > 5:
                    text += f"\n... 还有 {len(templates) - 5} 个话术"
            else:
                text += "暂无话术模板"
            
            await message.answer(
                text,
                reply_markup=Keyboards.dm_templates_menu(len(templates))
            )
        
        @self.dp.callback_query(F.data == "dm_tpl_type_forward")
        async def dm_tpl_type_forward(callback: CallbackQuery, state: FSMContext):
            """频道转发类型处理"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 标记为普通转发
            self.dm_template_temp[callback.from_user.id] = {'type': 'forward'}
            
            await callback.message.edit_text(
                "📢 频道转发\n\n"
                "请发送要转发的频道消息链接\n\n"
                "格式: https://t.me/频道用户名/消息ID\n"
                "示例: https://t.me/mychannel/123",
                reply_markup=Keyboards.back_to_dm_pool()
            )
            await state.set_state(DMStates.waiting_for_channel_link)
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_tpl_type_forward_hidden")
        async def dm_tpl_type_forward_hidden(callback: CallbackQuery, state: FSMContext):
            """隐藏来源转发类型处理"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 标记为隐藏来源转发
            self.dm_template_temp[callback.from_user.id] = {'type': 'forward_hidden'}
            
            await callback.message.edit_text(
                "👻 隐藏来源转发\n\n"
                "请发送要转发的频道消息链接\n"
                "转发时将不显示原始来源\n\n"
                "格式: https://t.me/频道用户名/消息ID\n"
                "示例: https://t.me/mychannel/123",
                reply_markup=Keyboards.back_to_dm_pool()
            )
            await state.set_state(DMStates.waiting_for_channel_link)
            await callback.answer()
        
        @self.dp.message(DMStates.waiting_for_channel_link)
        async def receive_channel_link(message: Message, state: FSMContext):
            """接收频道链接"""
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            link = message.text.strip()
            
            # 验证链接格式
            match = re.match(r'https?://t\.me/([^/]+)/(\d+)', link)
            if not match:
                await message.answer(
                    "❌ 链接格式错误\n\n"
                    "正确格式: https://t.me/频道用户名/消息ID\n"
                    "示例: https://t.me/mychannel/123"
                )
                return
            
            channel_username = match.group(1)
            message_id = match.group(2)
            
            # 获取转发类型
            temp_data = self.dm_template_temp.get(message.from_user.id, {})
            template_type = temp_data.get('type', 'forward')
            
            # 保存模板
            template_id = self.dm_template_manager.add_template(
                template_type=template_type,
                content={
                    'channel_link': link,
                    'channel_username': channel_username,
                    'message_id': message_id
                }
            )
            
            type_name = "隐藏来源转发" if template_type == 'forward_hidden' else "频道转发"
            
            await message.answer(
                f"✅ {type_name}话术已保存\n\n"
                f"频道: @{channel_username}\n"
                f"消息ID: {message_id}\n"
                f"模板ID: {template_id}"
            )
            
            # 清理临时数据
            if message.from_user.id in self.dm_template_temp:
                del self.dm_template_temp[message.from_user.id]
            
            await state.clear()
            
            # 返回话术管理菜单
            templates = self.dm_template_manager.get_all_templates()
            text = f"📝 私信话术管理\n\n"
            if templates:
                text += f"已配置话术 ({len(templates)}条):\n\n"
                type_names = {
                    'text': '📝 文本直发',
                    'postbot': '🖼️ 图文+按钮',
                    'forward': '📢 频道转发',
                    'forward_hidden': '👻 隐藏转发'
                }
                for tpl in templates[:5]:
                    tpl_type = tpl.get('type', 'text')
                    type_name = type_names.get(tpl_type, '未知')
                    text += f"{tpl.get('id')}. {type_name}\n"
                
                if len(templates) > 5:
                    text += f"\n... 还有 {len(templates) - 5} 个话术"
            else:
                text += "暂无话术模板"
            
            await message.answer(
                text,
                reply_markup=Keyboards.dm_templates_menu(len(templates))
            )
        
        @self.dp.callback_query(F.data.startswith("dm_tpl_detail_"))
        async def dm_tpl_detail(callback: CallbackQuery):
            """显示话术详情和删除按钮"""
            await callback.answer()
            
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.message.edit_text("⛔ 无权限访问")
                return
            
            try:
                template_id = int(callback.data.replace("dm_tpl_detail_", ""))
                template = self.dm_template_manager.get_template(template_id)
                
                if not template:
                    await callback.message.edit_text(
                        "❌ 话术不存在",
                        reply_markup=Keyboards.back_to_dm_pool()
                    )
                    return
                
                # 构建详情文本
                type_names = {
                    'text': '📝 文本直发',
                    'postbot': '🖼️ 图文+按钮',
                    'forward': '📢 频道转发',
                    'forward_hidden': '👻 隐藏转发'
                }
                
                tpl_type = template.get('type', 'text')
                type_name = type_names.get(tpl_type, '未知')
                content = template.get('content', {})
                
                text = f"📝 话术详情\n\n"
                text += f"ID: {template_id}\n"
                text += f"类型: {type_name}\n"
                text += f"创建时间: {template.get('created_at', 'N/A')}\n\n"
                
                if tpl_type == 'text':
                    text += f"内容:\n{content.get('text', '无')}\n\n"
                    text += f"防风控设置:\n"
                    text += f"• 随机Emoji: {'✅' if content.get('use_emoji') else '❌'}\n"
                    text += f"• 不可见字符: {'✅' if content.get('use_timestamp') else '❌'}\n"
                    text += f"• 同义词替换: {'✅' if content.get('use_synonym') else '❌'}"
                elif tpl_type in ['forward', 'forward_hidden']:
                    text += f"频道链接:\n{content.get('channel_link', '无')}"
                elif tpl_type == 'postbot':
                    text += f"PostBot 代码:\n{content.get('code', '无')}"
                
                # 创建删除按钮
                keyboard = [
                    [InlineKeyboardButton(text="🗑️ 删除话术", callback_data=f"dm_tpl_delete_{template_id}")],
                    [InlineKeyboardButton(text="🔙 返回列表", callback_data="dm_template_list")]
                ]
                
                await callback.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
                )
                
            except Exception as e:
                logger.error(f"显示话术详情失败: {e}")
                await callback.message.edit_text(
                    f"❌ 加载失败: {str(e)}",
                    reply_markup=Keyboards.back_to_dm_pool()
                )
        
        @self.dp.callback_query(F.data.startswith("dm_tpl_delete_"))
        async def dm_tpl_delete(callback: CallbackQuery):
            """删除话术"""
            await callback.answer()
            
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.message.edit_text("⛔ 无权限访问")
                return
            
            try:
                template_id = int(callback.data.replace("dm_tpl_delete_", ""))
                
                # 删除话术
                if self.dm_template_manager.remove_template(template_id):
                    await callback.answer("✅ 话术已删除", show_alert=True)
                    # 返回话术列表
                    await dm_template_list(callback)
                else:
                    await callback.answer("❌ 删除失败", show_alert=True)
                    
            except Exception as e:
                logger.error(f"删除话术失败: {e}")
                await callback.answer(f"❌ 删除失败: {str(e)}", show_alert=True)
            
            # 清理临时数据
            if callback.from_user.id in self.dm_template_temp:
                del self.dm_template_temp[callback.from_user.id]
            
            await callback.answer("✅ 话术已保存")
            await dm_templates(callback)
        
        # 处理用户发送贴纸 - 添加贴纸包
        @self.dp.message(F.sticker)
        async def handle_sticker(message: Message):
            """处理用户发送的贴纸 - 添加贴纸包"""
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            sticker = message.sticker
            set_name = sticker.set_name
            
            if not set_name:
                await message.answer("❌ 无法识别贴纸包")
                return
            
            if self.dm_sticker_manager.add_sticker_set(set_name):
                await message.answer(f"✅ 已添加贴纸包: {set_name}")
            else:
                await message.answer(f"ℹ️ 贴纸包已存在: {set_name}")
        
        @self.dp.callback_query(F.data == "dm_settings")
        async def dm_settings(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 清除任何活跃的FSM状态
            await state.clear()
            
            settings = self.dm_settings_manager.settings
            
            text = f"⏰ 发送频率设置\n\n"
            text += f"当前配置:\n"
            text += f"├── 随机延迟: {settings['delay_min']}-{settings['delay_max']} 秒\n"
            text += f"├── 批次大小: {settings['batch_size']} 条\n"
            text += f"├── 批次休息: {settings['batch_rest_min']//60}-{settings['batch_rest_max']//60} 分钟\n"
            text += f"├── 每日上限: {settings['daily_limit']} 条/账号\n"
            text += f"└── 活跃时段: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00"
            
            await self._safe_edit_message(
                callback.message,
                text,
                reply_markup=Keyboards.dm_send_config_menu(settings)
            )
            await callback.answer()
        
        # 延迟间隔配置
        @self.dp.callback_query(F.data == "dm_config_delay")
        async def dm_config_delay(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            settings = self.dm_settings_manager.settings
            
            await callback.message.edit_text(
                f"⏱️ 修改延迟间隔\n\n"
                f"请输入延迟范围（秒）\n"
                f"格式: 最小值|最大值\n"
                f"示例: 30|120\n\n"
                f"当前: {settings['delay_min']}-{settings['delay_max']}秒",
                reply_markup=Keyboards.cancel_config()
            )
            await state.set_state(SendConfigStates.waiting_delay)
            await callback.answer()
        
        @self.dp.message(SendConfigStates.waiting_delay)
        async def receive_delay_config(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                parts = message.text.strip().split('|')
                if len(parts) != 2:
                    raise ValueError("格式错误")
                
                delay_min = int(parts[0].strip())
                delay_max = int(parts[1].strip())
                
                if delay_min < 10 or delay_max > 600 or delay_min >= delay_max:
                    raise ValueError("数值范围错误")
                
                self.dm_settings_manager.update_setting('delay_min', delay_min)
                self.dm_settings_manager.update_setting('delay_max', delay_max)
                
                await message.answer(
                    f"✅ 延迟间隔已更新为 {delay_min}-{delay_max} 秒"
                )
                
                # 返回设置菜单
                settings = self.dm_settings_manager.settings
                text = f"⏰ 发送频率设置\n\n"
                text += f"当前配置:\n"
                text += f"├── 随机延迟: {settings['delay_min']}-{settings['delay_max']} 秒\n"
                text += f"├── 批次大小: {settings['batch_size']} 条\n"
                text += f"├── 批次休息: {settings['batch_rest_min']//60}-{settings['batch_rest_max']//60} 分钟\n"
                text += f"├── 每日上限: {settings['daily_limit']} 条/账号\n"
                text += f"└── 活跃时段: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00"
                
                await message.answer(text, reply_markup=Keyboards.dm_send_config_menu(settings))
                
            except Exception as e:
                await message.answer(
                    f"❌ 输入错误: {str(e)}\n\n请按格式输入: 最小值|最大值 (10-600秒)",
                    reply_markup=Keyboards.cancel_config()
                )
                return
            
            await state.clear()
        
        # 批次设置配置
        @self.dp.callback_query(F.data == "dm_config_batch")
        async def dm_config_batch(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            settings = self.dm_settings_manager.settings
            
            await callback.message.edit_text(
                f"📦 修改批次设置\n\n"
                f"请输入批次设置\n"
                f"格式: 批次大小|最小休息分钟|最大休息分钟\n"
                f"示例: 5|3|8\n\n"
                f"当前: {settings['batch_size']}条，休息{settings['batch_rest_min']//60}-{settings['batch_rest_max']//60}分钟",
                reply_markup=Keyboards.cancel_config()
            )
            await state.set_state(SendConfigStates.waiting_batch)
            await callback.answer()
        
        @self.dp.message(SendConfigStates.waiting_batch)
        async def receive_batch_config(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                parts = message.text.strip().split('|')
                if len(parts) != 3:
                    raise ValueError("格式错误")
                
                batch_size = int(parts[0].strip())
                rest_min = int(parts[1].strip())
                rest_max = int(parts[2].strip())
                
                if batch_size < 1 or batch_size > 20:
                    raise ValueError("批次大小应在1-20之间")
                if rest_min < 1 or rest_max > 60 or rest_min >= rest_max:
                    raise ValueError("休息时间范围错误")
                
                self.dm_settings_manager.update_setting('batch_size', batch_size)
                self.dm_settings_manager.update_setting('batch_rest_min', rest_min * 60)
                self.dm_settings_manager.update_setting('batch_rest_max', rest_max * 60)
                
                await message.answer(
                    f"✅ 批次设置已更新为 {batch_size}条，休息{rest_min}-{rest_max}分钟"
                )
                
                # 返回设置菜单
                settings = self.dm_settings_manager.settings
                text = f"⏰ 发送频率设置\n\n"
                text += f"当前配置:\n"
                text += f"├── 随机延迟: {settings['delay_min']}-{settings['delay_max']} 秒\n"
                text += f"├── 批次大小: {settings['batch_size']} 条\n"
                text += f"├── 批次休息: {settings['batch_rest_min']//60}-{settings['batch_rest_max']//60} 分钟\n"
                text += f"├── 每日上限: {settings['daily_limit']} 条/账号\n"
                text += f"└── 活跃时段: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00"
                
                await message.answer(text, reply_markup=Keyboards.dm_send_config_menu(settings))
                
            except Exception as e:
                await message.answer(
                    f"❌ 输入错误: {str(e)}\n\n请按格式输入: 批次大小|最小休息分钟|最大休息分钟",
                    reply_markup=Keyboards.cancel_config()
                )
                return
            
            await state.clear()
        
        # 每日上限配置
        @self.dp.callback_query(F.data == "dm_config_daily_limit")
        async def dm_config_daily_limit(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            settings = self.dm_settings_manager.settings
            
            await callback.message.edit_text(
                f"📊 修改每日上限\n\n"
                f"请输入每账号每日发送上限（条）\n"
                f"格式: 数字\n"
                f"示例: 50\n\n"
                f"当前: {settings['daily_limit']}条/账号",
                reply_markup=Keyboards.cancel_config()
            )
            await state.set_state(SendConfigStates.waiting_daily_limit)
            await callback.answer()
        
        @self.dp.message(SendConfigStates.waiting_daily_limit)
        async def receive_daily_limit_config(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                daily_limit = int(message.text.strip())
                
                if daily_limit < 1 or daily_limit > 200:
                    raise ValueError("每日上限应在1-200之间")
                
                self.dm_settings_manager.update_setting('daily_limit', daily_limit)
                
                await message.answer(
                    f"✅ 每日上限已更新为 {daily_limit}条/账号"
                )
                
                # 返回设置菜单
                settings = self.dm_settings_manager.settings
                text = f"⏰ 发送频率设置\n\n"
                text += f"当前配置:\n"
                text += f"├── 随机延迟: {settings['delay_min']}-{settings['delay_max']} 秒\n"
                text += f"├── 批次大小: {settings['batch_size']} 条\n"
                text += f"├── 批次休息: {settings['batch_rest_min']//60}-{settings['batch_rest_max']//60} 分钟\n"
                text += f"├── 每日上限: {settings['daily_limit']} 条/账号\n"
                text += f"└── 活跃时段: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00"
                
                await message.answer(text, reply_markup=Keyboards.dm_send_config_menu(settings))
                
            except Exception as e:
                await message.answer(
                    f"❌ 输入错误: {str(e)}\n\n请输入1-200之间的数字",
                    reply_markup=Keyboards.cancel_config()
                )
                return
            
            await state.clear()
        
        # 活跃时段配置
        @self.dp.callback_query(F.data == "dm_config_active_hours")
        async def dm_config_active_hours(callback: CallbackQuery, state: FSMContext):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            settings = self.dm_settings_manager.settings
            
            await callback.message.edit_text(
                f"🕐 修改活跃时段\n\n"
                f"请输入活跃时段（24小时制）\n"
                f"格式: 开始小时|结束小时\n"
                f"示例: 9|22\n\n"
                f"当前: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00",
                reply_markup=Keyboards.cancel_config()
            )
            await state.set_state(SendConfigStates.waiting_active_hours)
            await callback.answer()
        
        @self.dp.message(SendConfigStates.waiting_active_hours)
        async def receive_active_hours_config(message: Message, state: FSMContext):
            if message.from_user.id != Config.ADMIN_USER_ID:
                return
            
            try:
                parts = message.text.strip().split('|')
                if len(parts) != 2:
                    raise ValueError("格式错误")
                
                start_hour = int(parts[0].strip())
                end_hour = int(parts[1].strip())
                
                if start_hour < 0 or start_hour > 23 or end_hour < 0 or end_hour > 23:
                    raise ValueError("小时应在0-23之间")
                if start_hour >= end_hour:
                    raise ValueError("开始时间应早于结束时间")
                
                self.dm_settings_manager.update_setting('active_hours_start', start_hour)
                self.dm_settings_manager.update_setting('active_hours_end', end_hour)
                
                await message.answer(
                    f"✅ 活跃时段已更新为 {start_hour}:00-{end_hour}:00"
                )
                
                # 返回设置菜单
                settings = self.dm_settings_manager.settings
                text = f"⏰ 发送频率设置\n\n"
                text += f"当前配置:\n"
                text += f"├── 随机延迟: {settings['delay_min']}-{settings['delay_max']} 秒\n"
                text += f"├── 批次大小: {settings['batch_size']} 条\n"
                text += f"├── 批次休息: {settings['batch_rest_min']//60}-{settings['batch_rest_max']//60} 分钟\n"
                text += f"├── 每日上限: {settings['daily_limit']} 条/账号\n"
                text += f"└── 活跃时段: {settings['active_hours_start']}:00-{settings['active_hours_end']}:00"
                
                await message.answer(text, reply_markup=Keyboards.dm_send_config_menu(settings))
                
            except Exception as e:
                await message.answer(
                    f"❌ 输入错误: {str(e)}\n\n请按格式输入: 开始小时|结束小时 (0-23)",
                    reply_markup=Keyboards.cancel_config()
                )
                return
            
            await state.clear()
        
        # 贴纸打招呼设置
        @self.dp.callback_query(F.data == "dm_sticker_settings")
        async def dm_sticker_settings(callback: CallbackQuery):
            """贴纸打招呼设置"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            enabled = self.dm_settings_manager.get_setting('send_sticker_first')
            sticker_sets = self.dm_sticker_manager.get_all_sticker_sets()
            
            text = "🍒 贴纸打招呼设置\n\n"
            text += f"状态: {'✅ 开启' if enabled else '❌ 关闭'}\n\n"
            text += f"📦 已添加贴纸包 ({len(sticker_sets)}个):\n"
            
            if sticker_sets:
                for name in sticker_sets:
                    text += f"  • {name}\n"
            else:
                text += "  (无)\n"
            
            text += "\n💡 发送任意贴纸即可添加该贴纸包"
            
            keyboard = [
                [InlineKeyboardButton(
                    text="❌ 关闭贴纸打招呼" if enabled else "✅ 开启贴纸打招呼",
                    callback_data="dm_toggle_sticker"
                )],
                [InlineKeyboardButton(text="🗑️ 移除贴纸包", callback_data="dm_remove_sticker_set")],
                [InlineKeyboardButton(text="🔄 重置使用记录", callback_data="dm_reset_stickers")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="dm_settings")]
            ]
            
            await callback.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_toggle_sticker")
        async def dm_toggle_sticker(callback: CallbackQuery):
            """开关贴纸打招呼"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            current = self.dm_settings_manager.get_setting('send_sticker_first')
            self.dm_settings_manager.update_setting('send_sticker_first', not current)
            
            await callback.answer(f"{'✅ 已开启' if not current else '❌ 已关闭'}贴纸打招呼")
            await dm_sticker_settings(callback)
        
        @self.dp.callback_query(F.data == "dm_reset_stickers")
        async def dm_reset_stickers(callback: CallbackQuery):
            """重置贴纸使用记录"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            self.dm_sticker_manager.reset_used_stickers()
            await callback.answer("✅ 已重置贴纸使用记录", show_alert=True)
        
        @self.dp.callback_query(F.data == "dm_remove_sticker_set")
        async def dm_remove_sticker_set(callback: CallbackQuery):
            """移除贴纸包 - 显示列表"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            sticker_sets = self.dm_sticker_manager.get_all_sticker_sets()
            
            if not sticker_sets:
                await callback.answer("没有贴纸包可移除", show_alert=True)
                return
            
            keyboard = []
            for name in sticker_sets:
                keyboard.append([InlineKeyboardButton(
                    text=f"🗑️ {name}",
                    callback_data=f"dm_del_sticker_{name}"
                )])
            keyboard.append([InlineKeyboardButton(text="🔙 返回", callback_data="dm_sticker_settings")])
            
            await callback.message.edit_text(
                "选择要移除的贴纸包:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data.startswith("dm_del_sticker_"))
        async def dm_del_sticker(callback: CallbackQuery):
            """删除指定贴纸包"""
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            set_name = callback.data.replace("dm_del_sticker_", "")
            
            if self.dm_sticker_manager.remove_sticker_set(set_name):
                await callback.answer(f"✅ 已移除: {set_name}", show_alert=True)
            else:
                await callback.answer(f"❌ 移除失败", show_alert=True)
            
            await dm_sticker_settings(callback)
        
        @self.dp.callback_query(F.data == "dm_records")
        async def dm_records(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            stats = self.dm_record_manager.get_stats()
            recent = self.dm_record_manager.get_recent_records(10)
            
            text = f"📊 私信记录\n\n"
            text += f"今日统计:\n"
            text += f"• 发送: {stats['total_sent']}\n"
            text += f"• 成功: {stats['success']}\n"
            text += f"• 失败: {stats['failed']}\n"
            text += f"• 已私信用户: {stats['total_users']}\n\n"
            
            if recent:
                text += "最近记录:\n"
                for r in recent[-5:]:  # 最后5条
                    status = "✅" if r['status'] == 'success' else "❌"
                    username = r.get('username', '无')
                    text += f"{status} @{username}\n"
            
            # 添加清空按钮
            keyboard = [
                [InlineKeyboardButton(text="🗑️ 清空已私信列表", callback_data="dm_clear_sent_users")],
                [InlineKeyboardButton(text="🔙 返回", callback_data="menu_dm_pool")]
            ]
            
            await callback.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
        
        @self.dp.callback_query(F.data == "dm_clear_sent_users")
        async def dm_clear_sent_users(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            # 获取清空前的数量
            count = len(self.dm_record_manager.sent_users)
            
            # 清空列表
            self.dm_record_manager.clear_sent_users()
            
            await callback.answer(f"✅ 已清空 {count} 个已私信用户", show_alert=True)
            
            # 刷新页面
            await dm_records(callback)
        
        @self.dp.callback_query(F.data.startswith("dm_export_"))
        async def dm_export_accounts(callback: CallbackQuery):
            if callback.from_user.id != Config.ADMIN_USER_ID:
                await callback.answer("⛔ 无权限访问")
                return
            
            export_type = callback.data.replace("dm_export_", "")
            
            accounts = self.dm_account_manager.get_all_accounts()
            if not accounts:
                await callback.answer("❌ 没有账号可导出", show_alert=True)
                return
            
            # 根据类型筛选账号
            if export_type == 'all':
                filtered_accounts = accounts
                type_name = "全部账号"
                prefix = "all"
            elif export_type == 'normal':
                # 正常账号：只包含 active
                filtered_accounts = [acc for acc in accounts if acc.get('status') == 'active']
                type_name = "正常账号"
                prefix = "active"
            elif export_type == 'restricted':
                # 受限账号：包含 restricted 和 spam
                filtered_accounts = [acc for acc in accounts if acc.get('status') in ['restricted', 'spam']]
                type_name = "受限账号"
                prefix = "restricted"
            elif export_type == 'invalid':
                # 失效账号：包含 banned, frozen 和 failed
                filtered_accounts = [acc for acc in accounts if acc.get('status') in ['banned', 'frozen', 'failed']]
                type_name = "失效账号"
                prefix = "failed"
            else:
                await callback.answer("❌ 未知的导出类型", show_alert=True)
                return
            
            if not filtered_accounts:
                await callback.answer(f"❌ 没有{type_name}", show_alert=True)
                return
            
            # 状态消息
            status_msg = await callback.message.answer(f"⏳ 正在导出{type_name}...")
            
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # 1. 打包 session 文件
                zip_filename = os.path.join(Config.EXPORTS_DIR, f"{prefix}_sessions_{timestamp}.zip")
                session_count = 0
                
                with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for acc in filtered_accounts:
                        session_file = acc.get('session_file', '')
                        if not session_file:
                            continue
                        
                        # 构建session基础名（不含.session后缀）
                        session_base = session_file.replace('.session', '')
                        
                        # 查找所有相关文件（session, json等，跳过journal）
                        # 使用通配符匹配所有相关文件
                        pattern = os.path.join(Config.DM_SESSIONS_DIR, f"{session_base}*")
                        related_files = glob.glob(pattern)
                        
                        # 添加所有相关文件到ZIP（跳过journal）
                        for file_path in related_files:
                            if os.path.isfile(file_path):
                                file_name = os.path.basename(file_path)
                                
                                # 跳过 .session-journal 文件
                                if file_name.endswith('.session-journal'):
                                    continue
                                
                                zf.write(file_path, file_name)
                                if file_name.endswith('.session'):
                                    session_count += 1
                
                # 2. 生成账号列表 TXT
                txt_filename = os.path.join(Config.EXPORTS_DIR, f"{prefix}_accounts_{timestamp}.txt")
                
                # 状态标识映射
                status_emoji_map = {
                    'active': '✅', 'restricted': '⚠️', 'spam': '📵',
                    'banned': '🚫', 'frozen': '❄️', 'failed': '🔌', 'unknown': '❓'
                }
                
                status_text_map = {
                    'active': '无限制',
                    'restricted': '临时限制',
                    'spam': '垃圾邮件',
                    'banned': '封禁',
                    'frozen': '冻结',
                    'failed': '连接失败',
                    'unknown': '未知'
                }
                
                with open(txt_filename, 'w', encoding='utf-8') as f:
                    f.write(f"# {type_name}列表\n")
                    f.write(f"# 导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                    f.write(f"# 共 {len(filtered_accounts)} 个账号\n\n")
                    
                    for acc in filtered_accounts:
                        phone = acc.get('phone', '无')
                        status = acc.get('status', 'unknown')
                        status_emoji = status_emoji_map.get(status, '❓')
                        status_text = status_text_map.get(status, '未知')
                        
                        line = f"{phone} | {status_emoji} {status_text}"
                        
                        # 如果有限制截止时间，添加到信息中
                        if acc.get('limit_until'):
                            line += f" | 截止: {acc['limit_until']}"
                        
                        f.write(line + "\n")
                
                # 发送文件
                await status_msg.edit_text(f"📤 正在发送文件...")
                
                # 发送 ZIP 文件
                if session_count > 0:
                    await callback.message.answer_document(
                        FSInputFile(zip_filename),
                        caption=f"📦 Session文件打包 ({session_count}个)"
                    )
                
                # 发送 TXT 文件
                await callback.message.answer_document(
                    FSInputFile(txt_filename),
                    caption=f"📋 {type_name}列表 (共{len(filtered_accounts)}个)"
                )
                
                await status_msg.delete()
                await callback.answer(f"✅ 已导出 {len(filtered_accounts)} 个账号")
                
                # 删除已导出的账号
                deleted_count = 0
                for acc in filtered_accounts:
                    phone = acc['phone']
                    session_file = acc.get('session_file', '')
                    
                    try:
                        # 1. 断开客户端连接（如果已连接）
                        if phone in self.dm_clients:
                            try:
                                await self.dm_clients[phone].disconnect()
                                del self.dm_clients[phone]
                                logger.info(f"已断开私信号连接: {phone}")
                            except Exception as e:
                                logger.error(f"断开连接失败 {phone}: {e}")
                        
                        # 2. 删除所有相关文件
                        if session_file:
                            session_base = session_file.replace('.session', '')
                            
                            # 查找所有相关文件并删除
                            pattern = os.path.join(Config.DM_SESSIONS_DIR, f"{session_base}*")
                            related_files = glob.glob(pattern)
                            
                            for file_path in related_files:
                                if os.path.isfile(file_path):
                                    try:
                                        os.remove(file_path)
                                        logger.info(f"已删除文件: {os.path.basename(file_path)}")
                                    except Exception as e:
                                        logger.error(f"删除文件失败 {file_path}: {e}")
                        
                        # 3. 从账号列表中删除
                        if self.dm_account_manager.remove_account(phone):
                            deleted_count += 1
                            logger.info(f"已从账号列表删除: {phone}")
                    
                    except Exception as e:
                        logger.error(f"删除账号失败 {phone}: {e}")
                
                # 刷新DM号池菜单，显示最新数据
                dm_accounts = self.dm_account_manager.get_all_accounts()
                available_count = len([acc for acc in dm_accounts if acc.get('status') == 'active'])
                total_count = len(dm_accounts)
                stats = self.dm_record_manager.get_stats()
                
                enabled = self.dm_settings_manager.get_setting('enabled')
                text = f"✅ 导出完成！\n\n"
                text += f"📦 已导出: {len(filtered_accounts)} 个账号\n"
                text += f"🗑️ 已删除: {deleted_count} 个账号\n\n"
                text += f"━━━━━━━━━━━━━━━━━━\n"
                text += f"💬 私信号池管理\n\n"
                text += f"状态: {'✅ 已开启' if enabled else '❌ 已关闭'}\n"
                text += f"可用: {available_count} | 异常: {total_count - available_count} | 总计: {total_count}\n"
                text += f"今日私信: 发送 {stats['total_sent']} | 成功 {stats['success']} | 失败 {stats['failed']}"
                
                await callback.message.answer(
                    text,
                    reply_markup=Keyboards.dm_pool_menu(
                        enabled, available_count, total_count,
                        stats['total_sent'], stats['success'], stats['failed']
                    )
                )
                
                # 清理临时文件
                try:
                    if os.path.exists(zip_filename):
                        os.remove(zip_filename)
                    if os.path.exists(txt_filename):
                        os.remove(txt_filename)
                except Exception as e:
                    logger.error(f"清理导出文件失败: {e}")
                    
            except Exception as e:
                logger.error(f"导出账号失败: {e}", exc_info=True)
                await status_msg.edit_text(f"❌ 导出失败: {str(e)}")
                await callback.answer("❌ 导出失败", show_alert=True)
    
    def _update_dm_phone_hash_map(self):
        """更新DM phone hash映射"""
        self.dm_phone_hash_map.clear()
        for acc in self.dm_account_manager.get_all_accounts():
            phone_hash = abs(hash(acc['phone'])) % 100000
            self.dm_phone_hash_map[phone_hash] = acc['phone']
    
    async def start_multi_account_clients(self):
        """启动所有注册的监控账号"""
        accounts = self.account_manager.get_all_accounts()
        
        for acc in accounts:
            if not acc.get('enabled', True):
                continue
            
            phone = acc['phone']
            session_file = acc['session_file']
            session_path = os.path.join(Config.SESSIONS_DIR, session_file)
            
            try:
                client = TelegramClient(
                    session_path,
                    Config.API_ID,
                    Config.API_HASH,
                    proxy=self.proxy
                )
                
                await client.connect()
                
                if not await client.is_user_authorized():
                    logger.warning(f"账号 {phone} session 已过期，需要重新登录")
                    continue
                
                me = await client.get_me()
                logger.info(f"✅ 账号 {me.first_name} ({phone}) 已连接")
                
                self.clients[phone] = client
                
                @client.on(events.NewMessage())
                async def handle_msg(event):
                    await self.handle_new_message(event, phone)
                
            except Exception as e:
                logger.error(f"启动账号 {phone} 失败: {e}")
        
        logger.info(f"✅ 启动了 {len(self.clients)} 个监控账号")
    
    async def start_dm_clients(self):
        """启动所有私信号客户端"""
        accounts = self.dm_account_manager.get_all_accounts()
        
        for acc in accounts:
            phone = acc['phone']
            session_file = acc['session_file']
            session_path = os.path.join(Config.DM_SESSIONS_DIR, session_file.replace('.session', ''))
            
            try:
                # 尝试代理连接
                connection_type = 'unknown'
                client = None
                
                if self.proxy:
                    try:
                        client = TelegramClient(
                            session_path,
                            Config.API_ID,
                            Config.API_HASH,
                            proxy=self.proxy
                        )
                        await asyncio.wait_for(client.connect(), timeout=10)
                        connection_type = 'proxy'
                    except asyncio.TimeoutError:
                        logger.info(f"代理连接超时，尝试本地连接: {phone}")
                        if client:
                            await client.disconnect()
                        client = None
                
                if not client:
                    # 本地连接
                    client = TelegramClient(
                        session_path,
                        Config.API_ID,
                        Config.API_HASH
                    )
                    await client.connect()
                    connection_type = 'local'
                
                if not await client.is_user_authorized():
                    logger.warning(f"私信号 {phone} session 已过期")
                    self.dm_account_manager.update_account_status(phone, 'failed', False)
                    await client.disconnect()
                    continue
                
                me = await client.get_me()
                logger.info(f"✅ 私信号 {me.first_name} ({phone}) 已连接 [{connection_type}]")
                
                self.dm_clients[phone] = client
                
                # 更新连接状态
                self.dm_account_manager.update_account_status(phone, acc.get('status', 'active'), acc.get('can_send_dm', True))
                
            except Exception as e:
                logger.error(f"启动私信号 {phone} 失败: {e}")
                self.dm_account_manager.update_account_status(phone, 'failed', False)
        
        logger.info(f"✅ 启动了 {len(self.dm_clients)} 个私信号")
    
    async def handle_new_message(self, event, monitor_phone: str):
        """处理新消息 - 包含完整过滤逻辑"""
        try:
            receive_time = datetime.now()
            message = event.message
            self.stats['messages_received'] += 1
            
            # 添加消息接收时间日志
            logger.info(f"📩 收到消息 [{monitor_phone}] 时间: {receive_time.strftime('%H:%M:%S.%f')[:-3]}")
            
            # 消息去重：多个账号在同一群组时，同一消息只处理一次
            chat_id = event.chat_id
            msg_id = message.id
            msg_key = f"{chat_id}_{msg_id}"
            
            if msg_key in self.processed_messages:
                logger.debug(f"消息已处理，跳过: {msg_key}")
                return
            
            # 标记为已处理
            self.processed_messages[msg_key] = time.time()
            
            text = message.text or ''
            if not text:
                return
            
            # 获取发送者
            sender = await event.get_sender()
            if not isinstance(sender, User):
                return
            
            # 获取群组信息
            chat = await event.get_chat()
            chat_id = getattr(chat, 'id', 0)
            
            # 黑名单检查
            if self.blacklist_manager.is_user_blocked(sender.id):
                self.stats['filtered_count'] += 1
                logger.debug(f"用户已屏蔽: {sender.id}")
                return
            
            if self.blacklist_manager.is_chat_blocked(chat_id):
                self.stats['filtered_count'] += 1
                logger.debug(f"群组已屏蔽: {chat_id}")
                return
            
            # 消息长度过滤
            max_length = self.filter_manager.get_setting('max_message_length')
            if len(text) > max_length:
                self.stats['filtered_count'] += 1
                logger.debug(f"消息过长({len(text)}>{max_length})，已过滤")
                return
            
            # 匹配关键词
            matched_keywords = self.keyword_manager.match(text)
            if not matched_keywords:
                return
            
            # 用户过滤
            passed, reason = self.filter_manager.check_user_filter(sender)
            if not passed:
                self.stats['filtered_count'] += 1
                logger.info(f"用户过滤: {sender.id} - {reason}")
                return
            
            # 冷却检查
            for keyword in matched_keywords:
                cache_key = f"{sender.id}_{keyword}"
                if cache_key in self.cooldown_cache:
                    logger.debug(f"冷却中: {sender.id} - {keyword}")
                    continue
                
                self.cooldown_cache[cache_key] = time.time()
                self.stats['keywords_matched'] += 1
                
                # 构建转发消息
                forward_text = await self.build_forward_message(
                    chat, sender, message, [keyword], monitor_phone
                )
                
                # 获取群组 username（如果有）
                chat_username = getattr(chat, 'username', None)
                
                # 创建快捷操作按钮
                action_buttons = Keyboards.message_action_buttons(
                    chat_id=chat_id,
                    msg_id=message.id,
                    user_id=sender.id,
                    username=sender.username,
                    chat_username=chat_username
                )
                
                # 发送到监控群（带按钮）
                await self.bot.send_message(
                    chat_id=Config.MONITOR_CHAT_ID,
                    text=forward_text,
                    reply_markup=action_buttons,
                    parse_mode="HTML"
                )
                
                # 保存记录
                chat_title = getattr(chat, 'title', '私聊')
                
                self.record_manager.add_record(
                    user_id=sender.id,
                    username=sender.username or '',
                    name=f"{sender.first_name or ''} {sender.last_name or ''}".strip(),
                    chat_id=chat_id,
                    chat_title=chat_title,
                    keyword=keyword,
                    message=text,
                    monitor_account=monitor_phone
                )
                
                logger.info(f"✅ 转发: {keyword} from {sender.id} | 处理耗时: {(datetime.now() - receive_time).total_seconds():.2f}秒")
                
                # 触发自动私信流程（异步，不阻塞）- 传递完整的sender对象
                asyncio.create_task(self._auto_send_dm(sender))
                
        except Exception as e:
            logger.error(f"处理消息失败: {e}", exc_info=True)
    
    async def _auto_send_dm(self, sender):
        """自动私信流程"""
        try:
            user_id = sender.id
            username = sender.username or ''
            
            logger.info(f"📨 开始私信检查: 用户 {user_id} (@{username or '无'})")
            
            # 检查DM功能是否开启
            if not self.dm_settings_manager.get_setting('enabled'):
                logger.info(f"⏭️ 跳过私信: DM功能未开启")
                return
            
            # 检查用户是否有用户名
            if not username:
                logger.info(f"⏭️ 跳过私信: 用户 {user_id} 没有用户名")
                return
            
            # 检查用户是否已被私信过
            if self.dm_record_manager.is_user_sent(user_id):
                logger.info(f"⏭️ 跳过私信: 用户 {user_id} 已被私信过")
                return
            
            # 检查是否在活跃时段
            if not self.dm_settings_manager.is_active_hour():
                current_hour = datetime.now().hour
                start = self.dm_settings_manager.get_setting('active_hours_start')
                end = self.dm_settings_manager.get_setting('active_hours_end')
                logger.info(f"⏭️ 跳过私信: 当前{current_hour}点，活跃时段{start}-{end}点")
                return
            
            # 获取可用账号
            daily_limit = self.dm_settings_manager.get_setting('daily_limit')
            available_accounts = self.dm_account_manager.get_available_accounts(daily_limit)
            
            if not available_accounts:
                total = len(self.dm_account_manager.get_all_accounts())
                connected = len(self.dm_clients)
                logger.info(f"⏭️ 跳过私信: 没有可用私信号 (总数: {total}, 已连接: {connected})")
                return
            
            logger.info(f"✅ 私信条件检查通过，可用私信号: {len(available_accounts)} 个")
            
            # 随机选择一个账号
            dm_account = random.choice(available_accounts)
            dm_phone = dm_account['phone']
            
            logger.info(f"📱 选择私信号: {dm_phone}")
            
            # 获取DM客户端
            dm_client = self.dm_clients.get(dm_phone)
            if not dm_client or not dm_client.is_connected():
                logger.info(f"⏭️ 跳过私信: 私信号 {dm_phone} 未连接")
                return
            
            # 随机选择一个话术
            template = self.dm_template_manager.get_random_template()
            if not template:
                logger.info(f"⏭️ 跳过私信: 没有可用的话术模板")
                return
            
            logger.info(f"📝 选择话术: ID={template['id']}, 类型={template['type']}")
            
            # 随机延迟
            delay_min = self.dm_settings_manager.get_setting('delay_min')
            delay_max = self.dm_settings_manager.get_setting('delay_max')
            delay = random.randint(delay_min, delay_max)
            
            logger.info(f"将在 {delay}秒 后向用户 {user_id} 发送私信")
            await asyncio.sleep(delay)
            
            # 再次检查连接状态（延迟后可能断开）
            if not dm_client.is_connected():
                logger.warning(f"私信号在延迟后断开连接: {dm_phone}")
                # 尝试重新连接
                try:
                    await dm_client.connect()
                    if not await dm_client.is_user_authorized():
                        logger.error(f"私信号 {dm_phone} 未授权")
                        return
                    logger.info(f"私信号 {dm_phone} 重新连接成功")
                except Exception as e:
                    logger.error(f"重新连接失败 {dm_phone}: {e}")
                    return
            
            # 发送私信 - 传递完整的sender对象
            success = await self._send_dm_by_template(
                dm_client=dm_client,
                user=sender,  # 传递完整的user对象而不是user_id
                template=template
            )
            
            # 记录结果
            if success:
                self.dm_record_manager.add_sent_user(user_id)
                self.dm_account_manager.increment_sent_count(dm_phone)
                self.dm_record_manager.add_record(
                    user_id=user_id,
                    username=username,
                    dm_account=dm_phone,
                    template_id=template['id'],
                    template_type=template['type'],
                    status='success'
                )
                logger.info(f"✅ 私信发送成功: {user_id}")
                
                # 发送成功通知
                try:
                    stats = self.dm_record_manager.get_stats()
                    template_type_name = self._get_template_type_name(template['type'])
                    
                    # 生成话术内容预览
                    content_preview = ""
                    if template['type'] == 'text':
                        text_content = template['content'].get('text', '')
                        content_preview = text_content[:50] + ('...' if len(text_content) > 50 else '')
                    elif template['type'] == 'postbot':
                        content_preview = "图文消息"
                    elif template['type'] in ['forward', 'forward_hidden']:
                        content_preview = template['content'].get('channel_link', '')[:50]
                    
                    # 转义HTML特殊字符
                    from html import escape
                    dm_name = escape(dm_account.get('name', '未知'))
                    dm_username = escape(dm_account.get('username', '无'))
                    content_preview_escaped = escape(content_preview)
                    
                    # 创建可点击的用户名链接
                    if username:
                        user_mention = f'<a href="tg://user?id={user_id}">@{escape(username)}</a>'
                    else:
                        user_mention = 'N/A'
                    
                    notification = f"✅ 私信发送成功！\n\n"
                    notification += f"👤 目标用户: {user_mention} ({user_id})\n"
                    notification += f"📱 发送账号: {dm_name} (@{dm_username}) | {dm_phone}\n"
                    notification += f"💬 话术内容: {content_preview_escaped}\n"
                    notification += f"⏰ 发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    notification += f"━━━━━━━━━━━━━━━━━━\n"
                    notification += f"📊 今日统计: 发送 {stats['total_sent']} | 成功 {stats['success']} | 失败 {stats['failed']}"
                    
                    await self.bot.send_message(Config.ADMIN_USER_ID, notification, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"发送通知失败: {e}")
            else:
                self.dm_record_manager.add_record(
                    user_id=user_id,
                    username=username,
                    dm_account=dm_phone,
                    template_id=template['id'],
                    template_type=template['type'],
                    status='failed',
                    error='SEND_FAILED'
                )
                logger.warning(f"❌ 私信发送失败: {user_id}")
                
        except Exception as e:
            logger.error(f"自动私信失败: {e}", exc_info=True)
    
    def _get_template_type_name(self, type_code: str) -> str:
        """获取话术类型名称"""
        names = {
            "text": "文本直发",
            "postbot": "图文+按钮",
            "forward": "频道转发",
            "forward_hidden": "隐藏来源转发"
        }
        return names.get(type_code, type_code)
    
    def _create_mention_entities(self, text: str) -> List:
        """
        从文本中提取 @username 并创建 MessageEntityMention 实体
        这样 @mentions 会显示为可点击的蓝色链接
        
        Args:
            text: 要扫描的文本
            
        Returns:
            MessageEntityMention 实体列表
        """
        import re
        entities = []
        
        # 查找所有 @username 模式
        for match in re.finditer(r'@(\w+)', text):
            offset = match.start()
            length = len(match.group(0))
            entities.append(MessageEntityMention(offset, length))
        
        return entities if entities else None
    
    async def _send_dm_by_template(self, dm_client: TelegramClient, user, template: Dict) -> bool:
        """根据话术模板发送私信
        
        Args:
            dm_client: Telethon客户端
            user: 完整的用户对象（包含username）
            template: 话术模板
        """
        try:
            # 确保客户端已连接
            if not dm_client.is_connected():
                logger.error("DM客户端未连接")
                return False
            
            # 检查用户是否有用户名
            if not hasattr(user, 'username') or not user.username:
                logger.warning(f"用户 {getattr(user, 'id', 'Unknown')} 没有用户名，跳过发送")
                return False
            
            # 验证用户实体是否可被联系 - 使用用户名获取实体
            try:
                # 使用用户名获取实体，这样更可靠
                entity = await dm_client.get_entity(user.username)
                
                # 检查是否是机器人
                if entity.bot:
                    logger.warning(f"用户 @{user.username} 是机器人，无法发送消息")
                    return False
                
                # 检查 Peer 信息是否完整
                if not hasattr(entity, 'access_hash'):
                    logger.warning(f"用户 @{user.username} 的 Peer 信息不完整")
                    return False
                    
            except PeerIdInvalidError as e:
                logger.error(f"发送失败: 目标用户 @{user.username} 隐私限制或数据无效: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"验证用户实体失败 @{user.username}: {str(e)}")
                return False
            
            # 1️⃣ 先发贴纸打招呼（如果开启）
            if self.dm_settings_manager.get_setting('send_sticker_first'):
                try:
                    sticker = await self.dm_sticker_manager.get_random_sticker(dm_client)
                    if sticker:
                        await dm_client.send_file(entity, sticker)
                        logger.info(f"🍒 已发送贴纸打招呼")
                        
                        # 随机延迟
                        delay_min = self.dm_settings_manager.get_setting('sticker_delay_min') or 1.0
                        delay_max = self.dm_settings_manager.get_setting('sticker_delay_max') or 3.0
                        delay = random.uniform(delay_min, delay_max)
                        await asyncio.sleep(delay)
                except Exception as e:
                    logger.warning(f"发送贴纸失败: {e}")
            
            # 2️⃣ 发送话术内容
            template_type = template['type']
            content = template['content']
            
            if template_type == 'text':
                # 文本直发
                text = content['text']
                use_emoji = content.get('use_emoji', True)
                use_timestamp = content.get('use_timestamp', True)
                use_synonym = content.get('use_synonym', False)
                
                # 1. 先处理 Spintax 变体语法
                result = DMTemplateManager.process_spintax(text)
                
                # 2. 同义词替换（如果启用）
                if use_synonym:
                    pass  # TODO: 同义词替换逻辑
                
                # 3. 添加随机 Emoji
                if use_emoji:
                    result = DMTemplateManager.add_random_emoji(result)
                
                # 4. 添加不可见字符（防风控）
                if use_timestamp:
                    result = DMTemplateManager.add_invisible_timestamp(result)
                
                # 5. 最后转换 @username 为 HTML 可点击链接
                # 这一步必须在所有文本处理之后，避免零宽字符破坏 HTML 格式
                html_text = re.sub(r'@(\w+)', r'<a href="https://t.me/\1">@\1</a>', result)
                
                # 6. 发送消息（使用 HTML 解析模式）
                await dm_client.send_message(
                    entity,
                    html_text,
                    parse_mode='html',
                    link_preview=False  # 禁用链接预览，避免显示网页预览
                )
                logger.info(f"✅ 文本直发成功，@username 已转换为可点击链接")
                return True
                
            elif template_type == 'postbot':
                # 图文+按钮 (PostBot格式)
                postbot_code = content.get('code', '')
                if not postbot_code:
                    logger.error("PostBot 代码为空")
                    return False
                
                # 通过 PostBot 内联查询发送
                try:
                    from telethon.tl.functions.messages import GetInlineBotResultsRequest, SendInlineBotResultRequest
                    
                    # 获取 PostBot 实体
                    postbot = await dm_client.get_entity('@postbot')
                    
                    # 获取内联查询结果
                    results = await dm_client(GetInlineBotResultsRequest(
                        bot=postbot,
                        peer=entity,
                        query=postbot_code,
                        offset=''
                    ))
                    
                    if results.results:
                        # 发送第一个内联结果
                        await dm_client(SendInlineBotResultRequest(
                            peer=entity,
                            query_id=results.query_id,
                            id=results.results[0].id,
                            random_id=random.randint(0, 0x7fffffff)
                        ))
                        logger.info(f"PostBot 消息发送成功，代码: {postbot_code}")
                        return True
                    else:
                        logger.error(f"PostBot 未返回结果，代码可能无效: {postbot_code}")
                        return False
                        
                except Exception as e:
                    logger.error(f"PostBot 发送失败: {e}")
                    return False
                
            elif template_type == 'forward':
                # 频道转发
                channel_link = content.get('channel_link', '')
                # 解析频道链接: https://t.me/channel/123
                match = re.match(r'https?://t\.me/([^/]+)/(\d+)', channel_link)
                if not match:
                    logger.error(f"无效的频道链接: {channel_link}")
                    return False
                
                channel_username = match.group(1)
                message_id = int(match.group(2))
                
                # 获取频道实体
                channel_entity = await dm_client.get_entity(channel_username)
                
                # 转发消息 - 使用验证过的entity
                await dm_client.forward_messages(entity, message_id, channel_entity)
                return True
                
            elif template_type == 'forward_hidden':
                # 隐藏来源转发
                channel_link = content.get('channel_link', '')
                match = re.match(r'https?://t\.me/([^/]+)/(\d+)', channel_link)
                if not match:
                    logger.error(f"无效的频道链接: {channel_link}")
                    return False
                
                channel_username = match.group(1)
                message_id = int(match.group(2))
                
                # 获取频道实体
                channel_entity = await dm_client.get_entity(channel_username)
                
                # 获取原消息
                original_msg = await dm_client.get_messages(channel_entity, ids=message_id)
                
                if original_msg:
                    # 复制消息内容，保留格式实体
                    if original_msg.media:
                        # 有媒体的消息
                        await dm_client.send_message(
                            entity=entity,
                            message=original_msg.text or '',
                            formatting_entities=original_msg.entities,  # 保留@链接等
                            file=original_msg.media,
                            buttons=original_msg.reply_markup
                        )
                    else:
                        # 纯文本消息
                        await dm_client.send_message(
                            entity=entity,
                            message=original_msg.text or '',
                            formatting_entities=original_msg.entities  # 保留@链接等
                        )
                    return True
                
                return False
            
            return False
            
        except PeerIdInvalidError as e:
            logger.error(f"发送失败: 目标用户隐私限制或数据无效: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"发送私信失败: {e}", exc_info=True)
            return False
    
    async def build_forward_message(self, chat, sender: User, message, keywords: List[str], monitor_phone: str) -> str:
        """构建转发消息（使用HTML格式使@username可点击）"""
        if isinstance(chat, Channel):
            chat_title = chat.title
            chat_link = f"t.me/{chat.username}" if chat.username else "私有群组"
        elif isinstance(chat, Chat):
            chat_title = chat.title
            chat_link = "私有群组"
        else:
            chat_title = "私聊"
            chat_link = "私聊"
        
        sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "未知"
        # 使用HTML格式创建可点击的用户名链接
        if sender.username:
            sender_username = f'<a href="tg://user?id={sender.id}">@{sender.username}</a>'
        else:
            sender_username = "无"
        time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 转义HTML特殊字符
        from html import escape
        sender_name = escape(sender_name)
        chat_title = escape(chat_title)
        message_text = escape(message.text)
        keywords_text = escape(', '.join(keywords))
        
        text = "🔔 关键词触发提醒\n\n"
        text += f"📍 来源群组: {chat_title}\n"
        text += f"🔗 群组链接: {chat_link}\n"
        text += f"👤 发送用户: {sender_name} ({sender_username})\n"
        text += f"🆔 用户ID: {sender.id}\n"
        text += f"🔑 触发关键词: {keywords_text}\n"
        #text += f"📱 监控账号: {monitor_phone}\n"
        text += f"⏰ 时间: {time_str}\n\n"
        text += f"📝 消息内容:\n{message_text}"
        
        return text
    
    async def start(self):
        """启动机器人"""
        logger.info('=' * 50)
        logger.info('🤖 JTBot - 多账号监控系统')
        logger.info('=' * 50)
        
        # 启动已注册的监控账号
        await self.start_multi_account_clients()
        
        # 私信号客户端改为手动连接，不自动启动
        # await self.start_dm_clients()
        logger.info('💡 私信号需要手动连接，请在管理界面点击 [🔌 连接私信号] 按钮')
        
        # 启动 Bot
        logger.info('✅ Bot 管理界面已启动')
        
        try:
            # 创建任务
            bot_task = asyncio.create_task(self.dp.start_polling(self.bot))
            
            # 为每个监控客户端创建任务
            client_tasks = []
            for phone, client in self.clients.items():
                task = asyncio.create_task(client.run_until_disconnected())
                client_tasks.append(task)
            
            # 为每个DM客户端创建任务
            for phone, client in self.dm_clients.items():
                task = asyncio.create_task(client.run_until_disconnected())
                client_tasks.append(task)
            
            # 等待所有任务
            all_tasks = [bot_task] + client_tasks
            
            if all_tasks:
                done, pending = await asyncio.wait(
                    all_tasks,
                    return_when=asyncio.FIRST_EXCEPTION
                )
                
                # 取消未完成的任务
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                
                # 检查异常
                for task in done:
                    if task.exception():
                        raise task.exception()
                        
        except KeyboardInterrupt:
            logger.info('收到停止信号，正在关闭...')
        except Exception as e:
            logger.error(f'运行时错误: {e}', exc_info=True)
        finally:
            # 断开所有监控客户端
            for phone, client in self.clients.items():
                try:
                    await client.disconnect()
                except:
                    pass
            # 断开所有DM客户端
            for phone, client in self.dm_clients.items():
                try:
                    await client.disconnect()
                except:
                    pass
            logger.info('机器人已停止')


# ===== 程序入口 =====
async def main():
    """主函数"""
    try:
        bot = JTBot()
        await bot.start()
    except Exception as e:
        logger.error(f'❌ 程序异常: {e}', exc_info=True)


if __name__ == '__main__':
    asyncio.run(main())