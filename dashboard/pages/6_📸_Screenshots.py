import os
import streamlit as st
from common import (
    apply_custom_css,
    render_sidebar_header,
    get_screenshot_directory,
    SCREENSHOT_METADATA,
    render_screenshots_gallery,
)

st.set_page_config(
    page_title="Screenshots & Proof Gallery | SentinelFraud",
    page_icon="📸",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

st.title("📸 Pipeline Screenshots & Execution Evidence")
st.caption("Visual confirmation and terminal captures of services, tests, database queries, and benchmarks")

col_sel1, col_sel2 = st.columns([1, 2])
with col_sel1:
    st.markdown("### 🗂️ Select View Mode")
    view_mode = st.radio("Display Layout", ["Full Gallery (All Images)", "Individual Inspector (One by One)"])

screenshot_dir = get_screenshot_directory()
if screenshot_dir:
    images = sorted([
        f for f in os.listdir(screenshot_dir)
        if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))
    ])
    if view_mode == "Full Gallery (All Images)":
        render_screenshots_gallery()
    else:
        selected_img = st.selectbox("Choose Screenshot:", images, format_func=lambda x: SCREENSHOT_METADATA.get(x, {}).get("title", x))
        if selected_img:
            img_path = os.path.join(screenshot_dir, selected_img)
            info = SCREENSHOT_METADATA.get(selected_img, {"title": selected_img, "desc": "", "badge": "Capture"})
            st.markdown(f"### {info['title']}")
            st.markdown(f"*{info['desc']}*")
            st.image(img_path, caption=info['title'], use_container_width=True)
            st.caption(f"📁 Source: `{img_path}`")
else:
    st.warning("Screenshots directory not found.")
