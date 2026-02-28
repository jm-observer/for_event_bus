/// 将 interval tick 映射为业务事件类型 T。
pub trait FromTick {
    fn from_tick() -> Self;
}
