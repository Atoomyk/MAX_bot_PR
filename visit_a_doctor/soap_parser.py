# visit_a_doctor/soap_parser.py
"""
Модуль для парсинга SOAP-ответов от медицинского сервиса.
"""
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Any

class SoapResponseParser:
    """Парсер для обработки XML ответов SOAP сервиса"""

    @staticmethod
    def _extract_first_tag_value(xml_content: str, tag_name: str) -> Optional[str]:
        """
        Извлекает значение первого тега <tag_name>...</tag_name> из XML строки.
        Учитывает возможные namespace-префиксы (<v2:Tag>) и переносы/пробелы.
        """
        import re
        # (?:\w+:)? позволяет матчить <v2:Tag> и <Tag>
        pattern = rf"<(?:\w+:)?{re.escape(tag_name)}>\s*([\s\S]*?)\s*</(?:\w+:)?{re.escape(tag_name)}>"
        match = re.search(pattern, xml_content)
        if not match:
            return None
        value = match.group(1)
        return value.strip() if value is not None else None
    
    @staticmethod
    def parse_session_id(xml_content: str) -> Optional[str]:
        """Парсит Session_ID из любого ответа"""
        try:
            xml_clean = xml_content
            # Пытаемся удалить xmlns через регулярку для упрощения парсинга
            import re
            xml_clean = re.sub(r' xmlns:?[^=]*="[^"]+"', '', xml_clean) # remove xmlns="..."
            xml_clean = re.sub(r'<(\w+):', '<', xml_clean) # remove <prefix:
            xml_clean = re.sub(r'</(\w+):', '</', xml_clean) # remove </prefix:

            root = ET.fromstring(xml_clean)
            
            # Ищем Session_ID (он обычно на верхнем уровне тела ответа)
            # Примитивный поиск по всем тегам, так как неймспейсы могут мешать
            session_id = None
            if "Session_ID" in xml_content:
                # Fallback regex search if XML parsing is struggling with namespaces
                import re
                match = re.search(r'<Session_ID>([^<]+)</Session_ID>', xml_content)
                if match:
                    return match.group(1)
            
            # Попробуем через XML
            # Обычно: Body -> Response -> Session_ID
            # Но неймспейсы сложны. 
            pass 
        except Exception as e:
            print(f"Error parsing Session_ID: {e}")
        return None

    @staticmethod
    def parse_patient_id(xml_content: str) -> Optional[str]:
        """Парсит Patient_Id из GetPatientInfoResponse"""
        import re
        match = re.search(r'<Patient_Id>([^<]+)</Patient_Id>', xml_content)
        return match.group(1) if match else None

    @staticmethod
    def parse_mo_list(xml_content: str) -> List[Dict[str, str]]:
        """Парсит список МО (GetMOInfoExtendedResponse)"""
        mos = []
        try:
            # Очистка namespaces не всегда надежна, используем простой парсинг
            # Находим блок <MO_List>
            root = ET.fromstring(xml_content)
            # Для надежности используем итератор и ищем теги MO
            # Так как в примере <MO_List><MO>...</MO></MO_List>
            # А теги имеют префикс namespace, например ns2:MO
            
            # Простой вариант: найти все вхождения <MO>...</MO> с помощью split/regex
            # Но попробуем все же XML.
            
            # Удаляем namespaces из тегов перед парсингом для простоты
            import re
            xml_clean = re.sub(r' xmlns:?[^=]*="[^"]+"', '', xml_content)
            # также теги могут быть <ns:Tag>, уберем префиксы
            xml_clean = re.sub(r'<(\w+):', '<', xml_clean)
            xml_clean = re.sub(r'</(\w+):', '</', xml_clean)
            
            root = ET.fromstring(xml_clean)
            
            for mo_node in root.findall(".//MO"):
                mo_id = mo_node.find("MO_Id")
                name = mo_node.find("MO_Name")
                oid = mo_node.find("MO_OID")
                address = mo_node.find("MO_Address")
                
                if mo_id is not None and name is not None:
                    mos.append({
                        "id": mo_id.text,
                        "name": name.text,
                        "oid": oid.text if oid is not None else "",
                        "address": address.text if address is not None else ""
                    })

        except Exception as e:
            print(f"Error parsing MO list: {e}")
        return mos

    @staticmethod
    def parse_specialties(xml_content: str) -> List[Dict[str, str]]:
        """Парсит список специальностей (GetServicePostSpecsInfoResponse)"""
        specs = []
        try:
            import re
            xml_clean = re.sub(r' xmlns:?[^=]*="[^"]+"', '', xml_content)
            xml_clean = re.sub(r'<(\w+):', '<', xml_clean)
            xml_clean = re.sub(r'</(\w+):', '</', xml_clean)
            
            root = ET.fromstring(xml_clean)
            
            for post in root.findall(".//Post"):
                post_id = post.find("Post_Id")
                if post_id is not None:
                     specs.append({"id": post_id.text})
                     
        except Exception as e:
            print(f"Error parsing Specs: {e}")
        return specs

    @staticmethod
    def parse_doctors(xml_content: str) -> List[Dict[str, str]]:
        """Парсит список ресурсов (GetMOResourceInfoResponse)"""
        doctors = []
        try:
            import re
            xml_clean = re.sub(r' xmlns:?[^=]*="[^"]+"', '', xml_content)
            xml_clean = re.sub(r'<(\w+):', '<', xml_clean)
            xml_clean = re.sub(r'</(\w+):', '</', xml_clean)
            
            root = ET.fromstring(xml_clean)
            
            # Иерархия: MO_Resource_List -> MO_Available -> Resource_Available -> Resource -> Specialist или Room
            
            for resource in root.findall(".//Resource"):
                specialist = resource.find("Specialist")
                room = resource.find("Room")
                
                # Собираем доступные даты (общее для обоих случаев)
                dates = []
                avail_dates = resource.find("Available_Dates")
                if avail_dates is not None:
                     for d in avail_dates.findall("Available_Date"):
                         # 2025-12-19T00:00:00+03:00 -> 19.12.2025
                         raw_date = d.text[:10] # 2025-12-19
                         formatted_date = f"{raw_date[8:10]}.{raw_date[5:7]}.{raw_date[0:4]}"
                         dates.append(formatted_date)
                
                if not dates:  # Пропускаем ресурсы без доступных дат
                    continue
                
                # Случай 1: Есть Specialist (врач)
                if specialist is not None:
                    last = specialist.find("Last_Name")
                    first = specialist.find("First_Name")
                    middle = specialist.find("Middle_Name")
                    snils = specialist.find("SNILS")
                    
                    if last is not None and first is not None and middle is not None and snils is not None:
                        last_name = last.text or ""
                        first_name = first.text or ""
                        middle_name = middle.text or ""
                        snils_text = snils.text or ""
                        
                        full_name = f"{last_name} {first_name[0]}.{middle_name[0]}."
                        
                        doctors.append({
                            "id": snils_text, # ID врача для следующих шагов
                            "name": full_name,
                            "dates": dates,
                            "type": "specialist"  # Маркер типа ресурса
                        })
                
                # Случай 2: Есть только Room (кабинет без конкретного врача)
                elif room is not None:
                    room_id = room.find("Room_Id")
                    room_number = room.find("Room_Number")
                    room_name = room.find("Room_Name")
                    
                    if room_id is not None:
                        room_id_text = room_id.text or ""
                        room_number_text = room_number.text if room_number is not None else ""
                        room_name_text = room_name.text if room_name is not None else ""
                        
                        # Используем Room_Id как идентификатор, а Room_Name или Room_Number как имя
                        display_name = room_name_text if room_name_text else f"Кабинет {room_number_text}" if room_number_text else f"Кабинет {room_id_text}"
                        
                        doctors.append({
                            "id": f"ROOM_{room_id_text}", # Префикс ROOM_ для идентификации кабинета
                            "name": display_name,
                            "dates": dates,
                            "type": "room",  # Маркер типа ресурса
                            "room_id": room_id_text  # Сохраняем Room_Id для запроса слотов
                        })

        except Exception as e:
            print(f"Error parsing Doctors: {e}")
        return doctors

    @staticmethod
    def parse_slots(xml_content: str) -> List[Dict[str, str]]:
        """Парсит слоты (GetScheduleInfoResponse)"""
        slots = []
        try:
            import re
            xml_clean = re.sub(r' xmlns:?[^=]*="[^"]+"', '', xml_content)
            xml_clean = re.sub(r'<(\w+):', '<', xml_clean)
            xml_clean = re.sub(r'</(\w+):', '</', xml_clean)
            
            root = ET.fromstring(xml_clean)
            
            for slot in root.findall(".//Slots"):
                slot_id = slot.find("Slot_Id").text
                visit_time = slot.find("VisitTime").text # 2025-12-17T09:15:00+03:00
                room = slot.find("Room").text
                
                # Форматируем время: 09:15
                time_str = visit_time[11:16]
                
                slots.append({
                    "id": slot_id,
                    "time": time_str,
                    "room": room
                })

        except Exception as e:
            print(f"Error parsing Slots: {e}")
        return slots
        
    @staticmethod
    def parse_booking_status(xml_content: str) -> bool:
        """Парсит ответ создания записи (CreateAppointmentResponse)"""
        status_code = SoapResponseParser._extract_first_tag_value(xml_content, "Status_Code")
        return (status_code or "").strip().upper() == "SUCCESS"

    @staticmethod
    def parse_create_appointment_details(xml_content: str) -> Dict[str, Any]:
        """
        Парсит CreateAppointmentResponse и возвращает ключевые поля.

        Возвращает dict с ключами:
        - status_code: Optional[str]
        - book_id_mis: Optional[str]
        - visit_time: Optional[str] (как в XML, например 2025-12-17T09:15:00+03:00)
        - room: Optional[str]
        - slot_id: Optional[str]
        - session_id: Optional[str]
        """
        return {
            "status_code": SoapResponseParser._extract_first_tag_value(xml_content, "Status_Code"),
            "book_id_mis": SoapResponseParser._extract_first_tag_value(xml_content, "Book_Id_Mis"),
            "visit_time": SoapResponseParser._extract_first_tag_value(xml_content, "Visit_Time"),
            "room": SoapResponseParser._extract_first_tag_value(xml_content, "Room"),
            "slot_id": SoapResponseParser._extract_first_tag_value(xml_content, "Slot_Id"),
            "session_id": SoapResponseParser._extract_first_tag_value(xml_content, "Session_ID"),
        }
