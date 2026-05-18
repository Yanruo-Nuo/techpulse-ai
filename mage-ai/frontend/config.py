"""TechPulse AI 设计系统 tokens 与 CSS"""

# === Color Tokens (Data-Dense Dashboard 亮色主题) ===
COLORS = {
    "primary": "#1E40AF",
    "primary_light": "#2563EB",
    "accent": "#D97706",
    "bg_main": "#F8FAFC",
    "bg_card": "#FFFFFF",
    "bg_hover": "#F1F5F9",
    "text_primary": "#0F172A",
    "text_secondary": "#475569",
    "text_muted": "#94A3B8",
    "border": "#E2E8F0",
    "heat_high": "#16A34A",
    "heat_mid": "#CA8A04",
    "heat_low": "#9CA3AF",
    "destructive": "#DC2626",
    "chart_blue": "#3B82F6",
    "chart_amber": "#D97706",
    "chart_green": "#22C55E",
    "chart_purple": "#A855F7",
    "chart_cyan": "#06B6D4",
    "chart_rose": "#F43F5E",
    "chart_indigo": "#6366F1",
}

CATEGORY_COLORS = {
    "AI/ML": "#3B82F6",
    "Programming": "#22C55E",
    "CloudNative": "#06B6D4",
    "Security": "#DC2626",
    "Hardware": "#D97706",
    "DataEngineering": "#A855F7",
    "Others": "#64748B",
}

ITEMS_PER_PAGE = 10

CATEGORY_ORDER = [
    "AI/ML", "Programming", "CloudNative", "Security",
    "Hardware", "DataEngineering", "Others"
]


def get_css():
    """返回亮色主题全局 CSS"""
    return f"""
    <style>
        .stApp {{ background-color: {COLORS['bg_main']}; }}
        .stApp header {{ background-color: {COLORS['bg_main']}; }}

        .news-card {{
            background: {COLORS['bg_card']};
            border: 1px solid {COLORS['border']};
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 12px;
            transition: box-shadow 0.2s;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        }}
        .news-card:hover {{
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            background: {COLORS['bg_hover']};
        }}

        .heat-bar {{
            height: 4px;
            border-radius: 2px;
            margin-top: 8px;
            transition: width 0.3s;
        }}

        .tag {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
            margin-right: 4px;
        }}

        .cat-indicator {{
            width: 3px;
            height: 100%;
            min-height: 60px;
            border-radius: 2px;
            flex-shrink: 0;
        }}

        .topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 8px 0;
            margin-bottom: 16px;
            border-bottom: 1px solid {COLORS['border']};
        }}

        a {{ color: {COLORS['primary_light']}; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}

        .highlight {{ border-left: 3px solid {COLORS['accent']}; padding-left: 12px; }}

        ::-webkit-scrollbar {{ width: 6px; }}
        ::-webkit-scrollbar-track {{ background: {COLORS['bg_main']}; }}
        ::-webkit-scrollbar-thumb {{ background: {COLORS['border']}; border-radius: 3px; }}
    </style>
    """
