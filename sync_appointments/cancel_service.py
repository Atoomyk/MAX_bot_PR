"""
Сервис для отправки SOAP-запросов об отмене записи к врачу.

Важно: все параметры SOAP-запроса (URL, SOAPAction, заголовки, авторизация,
сертификаты и т.д.) нужно заполнить вручную перед использованием в бою.
"""

import logging
import os
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class CancelService:
    """
    Отправляет SOAP-запрос CancelAppointmentRequest во внешнюю систему.
    """

    # Берём endpoint из .env (SOAP_URL), чтобы отмена шла в тот же контур (тест/прод),
    # что и создание записи.
    # Fallback оставлен для старых конфигураций.
    SOAP_ENDPOINT_URL = os.getenv("SOAP_URL") or "http://192.168.240.26:8759/ws/rosminzdrav/fer3N/erwebservice_cc"
    # В рабочем примере SOAPAction не требовался, оставляем пустым
    SOAP_ACTION = ""
    DEFAULT_REASON = "CANCELED_BY_PATIENT"  # из предоставленного запроса

    # HTTP-заголовки можно дополнить (авторизация, токены и т.д.)
    BASE_HEADERS: Dict[str, str] = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "CancelAppointment"  # раскомментировать, если сервер этого требует
    }

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        soap_action: Optional[str] = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.endpoint_url = endpoint_url or self.SOAP_ENDPOINT_URL
        self.soap_action = soap_action or self.SOAP_ACTION
        self.timeout_seconds = timeout_seconds

    def _build_xml_body(
        self,
        book_id_mis: str,
        canceled_reason: str,
        error_data: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        """
        Формирует SOAP XML для CancelAppointmentRequest.
        """
        # Блок Error_Data_Parameters опционален и следует примеру с префиксом v2
        error_block = ""
        if error_data:
            msg = error_data.get("message") or ""
            path = error_data.get("path") or ""
            value = error_data.get("value") or ""
            error_block = (
                "<v2:Error_Data_Parameters>"
                "<v2:Parameter>"
                f"<v2:Message>{msg}</v2:Message>"
                f"<v2:Path>{path}</v2:Path>"
                f"<v2:Value>{value}</v2:Value>"
                "</v2:Parameter>"
                "</v2:Error_Data_Parameters>"
            )

        body = (
            "<v2:CancelAppointmentRequest>"
            f"<v2:Book_Id_Mis>{book_id_mis}</v2:Book_Id_Mis>"
            f"<v2:Canceled_Reason>{canceled_reason}</v2:Canceled_Reason>"
            f"{error_block}"
            "</v2:CancelAppointmentRequest>"
        )

        # Формируем SOAP Envelope как в предоставленном рабочем запросе
        envelope = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:v2="http://www.rt-eu.ru/med/er/v2_0">'
            "<soapenv:Header/>"
            f"<soapenv:Body>{body}</soapenv:Body>"
            "</soapenv:Envelope>"
        )
        return envelope

    async def send_cancel_request(
        self,
        book_id_mis: str,
        canceled_reason: Optional[str] = None,
        error_data: Optional[Dict[str, Optional[str]]] = None,
    ) -> Dict[str, Any]:
        """
        Отправляет SOAP-запрос отмены записи.

        Returns:
            Dict с ключами:
            - success: bool
            - status: HTTP-статус (если был запрос)
            - response: текст ответа (если был запрос)
            - error: описание ошибки (если была)
        """
        if not book_id_mis:
            return {"success": False, "error": "Отсутствует Book_Id_Mis"}

        if not self.endpoint_url or self.endpoint_url.startswith("<"):
            logger.warning("SOAP endpoint не настроен (endpoint_url пустой или содержит placeholder)")
            return {"success": False, "error": "SOAP endpoint не настроен"}

        reason = canceled_reason or self.DEFAULT_REASON
        payload = self._build_xml_body(book_id_mis, reason, error_data)

        # Готовим заголовки. Их нужно дополнить (авторизация, токены, сертификаты и т.д.)
        headers = dict(self.BASE_HEADERS)
        # Если интеграция требует SOAPAction в HTTP-заголовке — раскомментируйте:
        # headers["SOAPAction"] = self.soap_action

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint_url,
                    data=payload.encode("utf-8"),
                    headers=headers,
                    timeout=timeout,
                    # TODO: при необходимости добавить ssl=SSLContext(...) для клиентских сертификатов
                ) as response:
                    text = await response.text()
                    success = 200 <= response.status < 300

                    if success:
                        logger.info(
                            "SOAP отмена записи выполнена успешно: status=%s, Book_Id_Mis=%s",
                            response.status,
                            book_id_mis,
                        )
                    else:
                        logger.error(
                            "SOAP отмена записи завершилась ошибкой: status=%s, body=%s",
                            response.status,
                            text[:500],
                        )

                    return {
                        "success": success,
                        "status": response.status,
                        "response": text,
                    }

        except Exception as e:
            logger.error("Ошибка при отправке SOAP отмены записи: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

