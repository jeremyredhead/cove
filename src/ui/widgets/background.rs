use async_trait::async_trait;
use toss::{Frame, Pos, Size, Style};

use super::{BoxedWidget, Widget};

pub struct Background {
    inner: BoxedWidget,
    style: Style,
}

impl Background {
    pub fn new<W: Into<BoxedWidget>>(inner: W) -> Self {
        Self {
            inner: inner.into(),
            style: Style::new().opaque(),
        }
    }

    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

#[async_trait]
impl Widget for Background {
    fn size(&self, frame: &mut Frame, max_width: Option<u16>, max_height: Option<u16>) -> Size {
        self.inner.size(frame, max_width, max_height)
    }

    async fn render(self: Box<Self>, frame: &mut Frame) {
        let size = frame.size();
        for dy in 0..size.height {
            for dx in 0..size.width {
                frame.write(Pos::new(dx.into(), dy.into()), (" ", self.style));
            }
        }

        self.inner.render(frame).await;
    }
}
