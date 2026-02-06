# sync_appointments/scheduler.py
"""
Планировщик задач для автоматической синхронизации.
"""

import logging
from datetime import datetime, time
from typing import Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio

logger = logging.getLogger(__name__)


class SchedulerManager:
    """
    Менеджер планировщика задач для синхронизации записей.
    """

    def __init__(self, sync_service):
        """
        Инициализация планировщика.

        Args:
            sync_service: Сервис синхронизации (должен иметь метод run_sync())
        """
        self.sync_service = sync_service
        self.scheduler = AsyncIOScheduler()
        self.jobs = {}
        self.is_running = False

    def start_scheduler(self) -> bool:
        """
        Запускает планировщик с задачами по расписанию.

        Returns:
            True если планировщик успешно запущен
        """
        try:
            if self.is_running:
                logger.warning("Планировщик уже запущен")
                return True

            # 1. Ежедневная синхронизация в 08:50 по Москве
            sync_job = self.scheduler.add_job(
                func=self._run_sync_wrapper,
                trigger=CronTrigger(hour=8, minute=50, timezone='Europe/Moscow'),
                id='daily_sync',
                name='Ежедневная синхронизация записей',
                replace_existing=True
            )
            self.jobs['daily_sync'] = sync_job

            # 2. Еженедельная очистка старых записей в воскресенье в 03:00
            cleanup_job = self.scheduler.add_job(
                func=self._run_cleanup_wrapper,
                trigger=CronTrigger(day_of_week='sun', hour=3, minute=0, timezone='Europe/Moscow'),
                id='weekly_cleanup',
                name='Еженедельная очистка старых записей',
                replace_existing=True
            )
            self.jobs['weekly_cleanup'] = cleanup_job

            # 3. Ежечасная проверка статуса (опционально, для мониторинга)
            health_job = self.scheduler.add_job(
                func=self._health_check_wrapper,
                trigger=CronTrigger(minute=0),  # Каждый час в 0 минут
                id='hourly_health_check',
                name='Ежечасная проверка состояния',
                replace_existing=True
            )
            self.jobs['hourly_health_check'] = health_job

            # Запускаем планировщик
            self.scheduler.start()
            self.is_running = True

            logger.info("Планировщик запущен с задачами:")
            for job_id, job in self.jobs.items():
                next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "Не запланировано"
                logger.info(f"  • {job.name} (ID: {job_id}): следующее выполнение {next_run}")

            return True

        except Exception as e:
            logger.error(f"Ошибка запуска планировщика: {e}")
            return False

    async def _run_sync_wrapper(self):
        """
        Обертка для запуска синхронизации.
        """
        try:
            logger.info("Запуск запланированной синхронизации...")
            await self.sync_service.run_sync()
        except Exception as e:
            logger.error(f"Ошибка в запланированной синхронизации: {e}")

    async def _run_cleanup_wrapper(self):
        """
        Обертка для запуска очистки старых записей.
        """
        try:
            logger.info("Запуск запланированной очистки старых записей...")
            if hasattr(self.sync_service, 'run_cleanup'):
                await self.sync_service.run_cleanup()
            else:
                logger.warning("Метод run_cleanup не найден в sync_service")
        except Exception as e:
            logger.error(f"Ошибка в запланированной очистке: {e}")

    async def _health_check_wrapper(self):
        """
        Обертка для проверки состояния системы.
        """
        try:
            logger.debug("Выполнение проверки состояния системы...")
            # Можно добавить проверку доступности внешнего API, БД и т.д.
            if hasattr(self.sync_service, 'health_check'):
                await self.sync_service.health_check()
        except Exception as e:
            logger.debug(f"Ошибка проверки состояния: {e}")

    def stop_scheduler(self) -> bool:
        """
        Останавливает планировщик.

        Returns:
            True если планировщик успешно остановлен
        """
        try:
            if not self.is_running:
                logger.warning("Планировщик не запущен")
                return True

            self.scheduler.shutdown(wait=True)
            self.is_running = False
            self.jobs.clear()

            logger.info("Планировщик остановлен")
            return True

        except Exception as e:
            logger.error(f"Ошибка остановки планировщика: {e}")
            return False

    def run_manual_sync(self) -> bool:
        """
        Запускает ручную синхронизацию вне расписания.

        Returns:
            True если задача поставлена в очередь
        """
        try:
            # Добавляем задачу на немедленное выполнение
            manual_job = self.scheduler.add_job(
                func=self._run_sync_wrapper,
                trigger='date',
                id=f'manual_sync_{datetime.now().timestamp()}',
                name='Ручная синхронизация',
                replace_existing=True
            )

            logger.info("Ручная синхронизация поставлена в очередь")
            return True

        except Exception as e:
            logger.error(f"Ошибка запуска ручной синхронизации: {e}")
            return False

    def run_manual_cleanup(self) -> bool:
        """
        Запускает ручную очистку старых записей.

        Returns:
            True если задача поставлена в очередь
        """
        try:
            manual_job = self.scheduler.add_job(
                func=self._run_cleanup_wrapper,
                trigger='date',
                id=f'manual_cleanup_{datetime.now().timestamp()}',
                name='Ручная очистка',
                replace_existing=True
            )

            logger.info("Ручная очистка поставлена в очередь")
            return True

        except Exception as e:
            logger.error(f"Ошибка запуска ручной очистки: {e}")
            return False

    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        Возвращает статус планировщика и список задач.

        Returns:
            Словарь со статусом планировщика
        """
        status = {
            'is_running': self.is_running,
            'job_count': len(self.jobs),
            'jobs': {}
        }

        for job_id, job in self.jobs.items():
            status['jobs'][job_id] = {
                'name': job.name,
                'next_run': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None,
                'trigger': str(job.trigger)
            }

        return status

    def reschedule_job(self, job_id: str, cron_expression: str) -> bool:
        """
        Изменяет расписание задачи.

        Args:
            job_id: ID задачи
            cron_expression: Выражение cron (например: "0 8 * * *")

        Returns:
            True если расписание успешно изменено
        """
        try:
            if job_id not in self.jobs:
                logger.error(f"Задача с ID {job_id} не найдена")
                return False

            job = self.jobs[job_id]

            # Удаляем старую задачу
            job.remove()

            # Создаем новую с новым расписанием
            new_job = self.scheduler.add_job(
                func=job.func,
                trigger=CronTrigger.from_crontab(cron_expression, timezone='Europe/Moscow'),
                id=job_id,
                name=job.name,
                replace_existing=True
            )

            self.jobs[job_id] = new_job

            next_run = new_job.next_run_time.strftime(
                '%Y-%m-%d %H:%M:%S') if new_job.next_run_time else "Не запланировано"
            logger.info(f"Расписание задачи {job_id} изменено. Следующее выполнение: {next_run}")

            return True

        except Exception as e:
            logger.error(f"Ошибка изменения расписания задачи {job_id}: {e}")
            return False

    def pause_job(self, job_id: str) -> bool:
        """
        Приостанавливает задачу.

        Args:
            job_id: ID задачи

        Returns:
            True если задача успешно приостановлена
        """
        try:
            if job_id not in self.jobs:
                logger.error(f"Задача с ID {job_id} не найдена")
                return False

            self.jobs[job_id].pause()
            logger.info(f"Задача {job_id} приостановлена")
            return True

        except Exception as e:
            logger.error(f"Ошибка приостановки задачи {job_id}: {e}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """
        Возобновляет приостановленную задачу.

        Args:
            job_id: ID задачи

        Returns:
            True если задача успешно возобновлена
        """
        try:
            if job_id not in self.jobs:
                logger.error(f"Задача с ID {job_id} не найдена")
                return False

            self.jobs[job_id].resume()
            logger.info(f"Задача {job_id} возобновлена")
            return True

        except Exception as e:
            logger.error(f"Ошибка возобновления задачи {job_id}: {e}")
            return False

    async def wait_for_scheduler(self):
        """
        Ожидает завершения всех задач планировщика.
        Используется при завершении работы приложения.
        """
        try:
            if self.is_running:
                logger.info("Ожидание завершения задач планировщика...")
                self.scheduler.shutdown(wait=True)
                await asyncio.sleep(1)  # Даем время на завершение
        except Exception as e:
            logger.error(f"Ошибка ожидания планировщика: {e}")