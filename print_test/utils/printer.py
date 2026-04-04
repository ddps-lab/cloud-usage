"""구조화된 출력 관리"""
from pprint import PrettyPrinter
from typing import Dict, Any, List


class StructuredPrinter:
    """구조화된 출력 관리"""

    def __init__(self):
        self.pp = PrettyPrinter(indent=2, width=100, compact=False)

    def print_header(self, title: str, api_name: str = ""):
        """API 섹션 헤더"""
        print("\n" + "=" * 80)
        print(f"📊 {title}")
        if api_name:
            print(f"   API: {api_name}")
        print("=" * 80)

    def print_section(self, section_title: str):
        """섹션 제목"""
        print(f"\n▶ {section_title}")
        print("-" * 80)

    def print_response(self, response: Dict[str, Any], max_items: int = 100):
        """응답 출력 (아이템 제한)"""
        if isinstance(response, dict):
            display_response = self._truncate_response(response, max_items)
            self.pp.pprint(display_response)
        else:
            self.pp.pprint(response)

    def _truncate_response(self, obj: Any, max_items: int) -> Any:
        """대용량 리스트 축약"""
        if isinstance(obj, dict):
            return {k: self._truncate_response(v, max_items) for k, v in obj.items()}
        elif isinstance(obj, list):
            truncated = obj[:max_items]
            if len(obj) > max_items:
                truncated.append(f"... ({len(obj) - max_items}개 더 있음)")
            return truncated
        else:
            return obj

    def print_key_info(self, info_dict: Dict[str, Any]):
        """핵심 정보 테이블 형식"""
        print("\n📋 핵심 정보:")
        for key, value in info_dict.items():
            if isinstance(value, (dict, list)):
                print(f"  {key}:")
                for item in (value if isinstance(value, list) else [value]):
                    print(f"    - {item}")
            else:
                print(f"  {key}: {value}")

    def print_parsing_tips(self, tips: List[str]):
        """파싱 팁"""
        print("\n💡 파싱 팁:")
        for tip in tips:
            print(f"  ✓ {tip}")
