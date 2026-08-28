use std::collections::BTreeMap;

/// A generic map from non-overlapping `(start, end)` slot ranges to values,
/// backed by a [`BTreeMap`] keyed on range end for O(log n) point lookup.
#[derive(Debug, Clone)]
pub(crate) struct SlotRangeMap<V> {
    inner: BTreeMap<u16, (u16, V)>, // end → (start, value)
}

impl<V> SlotRangeMap<V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Insert a value for the slot range `[start, end]` (inclusive).
    pub fn insert(&mut self, start: u16, end: u16, value: V) {
        self.inner.insert(end, (start, value));
    }

    /// Look up the value whose range contains `slot`, if any.
    pub fn get(&self, slot: u16) -> Option<&V> {
        self.inner.range(slot..).next().and_then(
            |(_, (start, v))| {
                if slot >= *start { Some(v) } else { None }
            },
        )
    }

    /// Bounds of the range containing `slot`, if any.
    pub fn range_containing(&self, slot: u16) -> Option<(u16, u16)> {
        self.inner
            .range(slot..)
            .next()
            .and_then(|(&end, (start, _))| (slot >= *start).then_some((*start, end)))
    }

    /// Remove the range ending at `end`, returning its start and value.
    pub fn remove_range(&mut self, end: u16) -> Option<(u16, V)> {
        self.inner.remove(&end)
    }

    /// Iterate over all values (one per range entry, in slot order).
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values().map(|(_, v)| v)
    }

    /// Iterate over `(start, end, &value)` triples in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (u16, u16, &V)> {
        self.inner.iter().map(|(&end, (start, v))| (*start, end, v))
    }

    /// Replace individual points in one ordered pass and merge adjacent equal ranges.
    pub fn replace_points(&mut self, points: BTreeMap<u16, V>)
    where
        V: Clone + PartialEq,
    {
        if points.is_empty() {
            return;
        }

        let ranges = std::mem::take(&mut self.inner);
        let mut points = points.into_iter().peekable();
        let mut replaced = BTreeMap::new();
        let mut current: Option<(u16, u16, V)> = None;

        let mut append =
            |start, end, value, current: &mut Option<(u16, u16, V)>| match current.take() {
                Some((current_start, current_end, current_value))
                    if current_end.checked_add(1) == Some(start) && current_value == value =>
                {
                    *current = Some((current_start, end, current_value));
                }
                Some((current_start, current_end, current_value)) => {
                    replaced.insert(current_end, (current_start, current_value));
                    *current = Some((start, end, value));
                }
                None => *current = Some((start, end, value)),
            };

        for (end, (start, value)) in ranges {
            while let Some((slot, replacement)) = points.next_if(|(slot, _)| *slot < start) {
                append(slot, slot, replacement, &mut current);
            }

            let mut unchanged_start = u32::from(start);
            while let Some((slot, replacement)) = points.next_if(|(slot, _)| *slot <= end) {
                let slot = u32::from(slot);
                if unchanged_start < slot {
                    append(
                        unchanged_start as u16,
                        (slot - 1) as u16,
                        value.clone(),
                        &mut current,
                    );
                }
                append(slot as u16, slot as u16, replacement, &mut current);
                unchanged_start = slot + 1;
            }
            if unchanged_start <= u32::from(end) {
                append(unchanged_start as u16, end, value, &mut current);
            }
        }

        for (slot, replacement) in points {
            append(slot, slot, replacement, &mut current);
        }
        if let Some((start, end, value)) = current {
            replaced.insert(end, (start, value));
        }
        self.inner = replaced;
    }

    #[cfg(feature = "cluster-async")]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<V> Default for SlotRangeMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_lookup_within_range() {
        let mut m = SlotRangeMap::new();
        m.insert(100, 200, "a");
        m.insert(300, 400, "b");

        assert_eq!(m.get(100), Some(&"a"));
        assert_eq!(m.get(150), Some(&"a"));
        assert_eq!(m.get(200), Some(&"a"));
        assert_eq!(m.get(300), Some(&"b"));
        assert_eq!(m.get(350), Some(&"b"));
        assert_eq!(m.get(400), Some(&"b"));
    }

    #[test]
    fn point_lookup_in_gaps_returns_none() {
        let mut m = SlotRangeMap::new();
        m.insert(100, 200, "a");
        m.insert(300, 400, "b");

        assert_eq!(m.get(99), None);
        assert_eq!(m.get(250), None);
        assert_eq!(m.get(401), None);
    }

    #[test]
    fn range_containing_reports_bounds_or_none() {
        let mut m = SlotRangeMap::new();
        m.insert(100, 200, "a");
        m.insert(300, 400, "b");

        assert_eq!(m.range_containing(100), Some((100, 200)));
        assert_eq!(m.range_containing(150), Some((100, 200)));
        assert_eq!(m.range_containing(200), Some((100, 200)));
        assert_eq!(m.range_containing(400), Some((300, 400)));
        assert_eq!(m.range_containing(250), None);
        assert_eq!(m.range_containing(401), None);
    }

    #[test]
    fn remove_range_takes_the_entry_by_end() {
        let mut m = SlotRangeMap::new();
        m.insert(100, 200, "a");
        m.insert(300, 400, "b");

        assert_eq!(m.remove_range(200), Some((100, "a")));
        assert_eq!(m.get(150), None);
        assert_eq!(m.get(350), Some(&"b"));
        assert_eq!(m.remove_range(200), None);
    }

    #[test]
    fn iter_yields_ranges_in_order() {
        let mut m = SlotRangeMap::new();
        m.insert(300, 400, "b");
        m.insert(100, 200, "a");

        let entries: Vec<_> = m.iter().collect();
        assert_eq!(entries, vec![(100, 200, &"a"), (300, 400, &"b")]);
    }

    #[test]
    fn replace_points_splits_ranges_and_merges_without_crossing_gaps() {
        let mut m = SlotRangeMap::new();
        m.insert(0, 9, "a");
        m.insert(20, 29, "b");

        m.replace_points(BTreeMap::from([
            (0, "c"),
            (5, "c"),
            (9, "b"),
            (10, "b"),
            (19, "b"),
            (20, "b"),
            (30, "b"),
        ]));

        assert_eq!(
            m.iter().collect::<Vec<_>>(),
            vec![
                (0, 0, &"c"),
                (1, 4, &"a"),
                (5, 5, &"c"),
                (6, 8, &"a"),
                (9, 10, &"b"),
                (19, 30, &"b"),
            ]
        );
    }
}
