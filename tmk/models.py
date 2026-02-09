"""
Pydantic модели для API интеграции с МИС
"""
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum


class SexEnum(str, Enum):
    """Пол пациента"""
    MALE = "1"
    FEMALE = "2"


class StatusEnum(str, Enum):
    """Статус консультации"""
    APPROVED = "APPROVED"
    CANCELLED = "CANCELLED"


class PayMethodEnum(str, Enum):
    """Метод оплаты"""
    OMS = "OMS"
    DMS = "DMS"
    PAID = "PAID"


class OMSData(BaseModel):
    """Данные полиса ОМС"""
    number: str = Field(..., description="Номер полиса ОМС")
    series: Optional[str] = Field(None, description="Серия полиса (старый формат)")


class DoctorData(BaseModel):
    """Данные врача"""
    firstName: str = Field(..., description="Имя врача")
    lastName: str = Field(..., description="Фамилия врача")
    middleName: Optional[str] = Field(None, description="Отчество врача")
    specialization: str = Field(..., description="Специализация")
    position: Optional[str] = Field(None, description="Должность")
    SNILS: str = Field(..., description="СНИЛС врача")
    
    def get_full_name(self) -> str:
        """Получить полное ФИО"""
        parts = [self.lastName, self.firstName]
        if self.middleName:
            parts.append(self.middleName)
        return " ".join(parts)


class PatientData(BaseModel):
    """Данные пациента"""
    firstName: str = Field(..., description="Имя пациента")
    lastName: str = Field(..., description="Фамилия пациента")
    middleName: Optional[str] = Field(None, description="Отчество пациента")
    birthDate: str = Field(..., description="Дата рождения в формате YYYY-MM-DD")
    OMS: OMSData = Field(..., description="Данные полиса ОМС")
    SNILS: str = Field(..., description="СНИЛС пациента")
    sex: SexEnum = Field(..., description="Пол (1 - мужской, 2 - женский)")
    phone: Optional[str] = Field(None, description="Номер телефона (если есть)")
    
    def get_full_name(self) -> str:
        """Получить полное ФИО"""
        parts = [self.lastName, self.firstName]
        if self.middleName:
            parts.append(self.middleName)
        return " ".join(parts)


class ClinicData(BaseModel):
    """Данные медицинской организации"""
    name: str = Field(..., description="Название МО")
    address: str = Field(..., description="Адрес МО")
    MO_OID: str = Field(..., description="OID медицинской организации")
    phone: str = Field(..., description="Телефон МО")


class RoomData(BaseModel):
    """Данные кабинета (вариант запроса без врача)"""
    name: str = Field(..., description="Название кабинета")
    id: str = Field(..., description="ID кабинета")
    specialization: str = Field(..., description="Специализация")
    position: str = Field(..., description="Должность (используется в названии ТМК)")


class TelemedCreateRequest(BaseModel):
    """Запрос на создание ТМК от МИС. Передаётся либо doctor, либо room (ровно одно)."""
    externalId: str = Field(..., description="Внешний ID консультации из МИС")
    scheduleDate: str = Field(..., description="Дата и время консультации")
    doctor: Optional[DoctorData] = Field(None, description="Данные врача")
    room: Optional[RoomData] = Field(None, description="Данные кабинета (вместо врача)")
    patient: PatientData = Field(..., description="Данные пациента")
    clinic: ClinicData = Field(..., description="Данные клиники")
    status: StatusEnum = Field(..., description="Статус консультации")
    payMethod: PayMethodEnum = Field(..., description="Метод оплаты")

    @model_validator(mode="after")
    def doctor_or_room_exactly_one(self):
        if (self.doctor is None) == (self.room is None):
            raise ValueError("Должно быть указано ровно одно: doctor или room")
        return self


class TelemedUpdateRequest(BaseModel):
    """Запрос на обновление/отмену ТМК от МИС. Передаётся либо doctor, либо room (тот же тип, что при создании)."""
    scheduleDate: str = Field(..., description="Дата и время консультации")
    status: StatusEnum = Field(..., description="Новый статус консультации")
    doctor: Optional[DoctorData] = Field(None, description="Данные врача")
    room: Optional[RoomData] = Field(None, description="Данные кабинета (вместо врача)")

    @model_validator(mode="after")
    def doctor_or_room_exactly_one(self):
        if (self.doctor is None) == (self.room is None):
            raise ValueError("Должно быть указано ровно одно: doctor или room")
        return self


class TelemedCreateResponse(BaseModel):
    """Ответ на создание ТМК"""
    status: str = Field(..., description="Статус операции (success/error)")
    id: str = Field(..., description="Внутренний ID сессии ТМК")
    externalId: str = Field(..., description="Внешний ID консультации из МИС")
    chat_invite_link: Optional[str] = Field(None, description="Ссылка на чат")
    message: str = Field(..., description="Сообщение о результате")
    error: Optional[str] = Field(None, description="Описание ошибки")


class TelemedUpdateResponse(BaseModel):
    """Ответ на обновление ТМК"""
    status: str = Field(..., description="Статус операции (success/error)")
    id: str = Field(..., description="ID сессии ТМК")
    externalId: str = Field(..., description="Внешний ID консультации из МИС")
    message: str = Field(..., description="Сообщение о результате")
    error: Optional[str] = Field(None, description="Описание ошибки")


class MaxChatResponse(BaseModel):
    """Ответ от MAX API при создании чата"""
    all_can_post: bool = Field(..., description="Могут ли все писать в чате")
    chat_id: int = Field(..., description="ID созданного чата")
    invite_link: str = Field(..., description="Ссылка для присоединения к чату")
