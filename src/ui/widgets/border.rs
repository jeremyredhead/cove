use async_trait::async_trait;
use crossterm::style::ContentStyle;
use toss::frame::{Frame, Pos, Size};

use super::{BoxedWidget, Widget};

pub struct Border {
    inner: BoxedWidget,
    style: ContentStyle,
}

impl Border {
    pub fn new<W: Into<BoxedWidget>>(inner: W) -> Self {
        Self {
            inner: inner.into(),
            style: ContentStyle::default(),
        }
    }

    pub fn style(mut self, style: ContentStyle) -> Self {
        self.style = style;
        self
    }
}

#[async_trait]
impl Widget for Border {
    fn size(&self, frame: &mut Frame, max_width: Option<u16>, max_height: Option<u16>) -> Size {
        let max_width = max_width.map(|w| w.saturating_sub(2));
        let max_height = max_height.map(|h| h.saturating_sub(2));
        let size = self.inner.size(frame, max_width, max_height);
        size + Size::new(2, 2)
    }

    async fn render(self: Box<Self>, frame: &mut Frame) {
        let mut size = frame.size();
        size.width = size.width.max(2);
        size.height = size.height.max(2);

        let right = size.width as i32 - 1;
        let bottom = size.height as i32 - 1;
        frame.write(Pos::new(0, 0), ("┌", self.style));
        frame.write(Pos::new(right, 0), ("┐", self.style));
        frame.write(Pos::new(0, bottom), ("└", self.style));
        frame.write(Pos::new(right, bottom), ("┘", self.style));

        for y in 1..bottom {
            frame.write(Pos::new(0, y), ("│", self.style));
            frame.write(Pos::new(right, y), ("│", self.style));
        }

        for x in 1..right {
            frame.write(Pos::new(x, 0), ("─", self.style));
            frame.write(Pos::new(x, bottom), ("─", self.style));
        }

        frame.push(Pos::new(1, 1), size - Size::new(2, 2));
        self.inner.render(frame).await;
        frame.pop();
    }
}
