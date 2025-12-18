export const  getWeekOfMonth = (date: number, month: number, year: number) => {
    const firstDay = new Date(year, month, 1);
    const firstWeekday = firstDay.getDay(); // 0 (Sun) to 6 (Sat)
    // Calculate week number: (Day + Weekday of the 1st) / 7
    return Math.ceil((date + firstWeekday) / 7);
}