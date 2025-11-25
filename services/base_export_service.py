from abc import ABC, abstractmethod
from typing import Dict, List, Any, Tuple, Set
import time
import gzip
import json
import os
from datetime import datetime
from collections import defaultdict

from .hierarchical_tag_counter import HierarchicalTagCounter

class BaseExportService(ABC):
    """Base class for all export services with hierarchical counting"""
    
    def __init__(self):
        self.start_time = None
        self.cards_by_id = {}
        self.notes_by_id = {}
        self.revlog_by_card = {}
        self.tag_counter = HierarchicalTagCounter()
        self.service_name = self.get_service_name()
        
    @abstractmethod
    def get_service_name(self) -> str:
        """Get the name for this service"""
        pass
    
    @abstractmethod
    def get_filter_criteria(self) -> Dict[str, Any]:
        """Define what data to include in this export"""
        pass
    
    def export_data(self) -> Tuple[str, int]:
        """Main export method with hierarchical counting - maintains 1.6s performance"""
        self.start_time = time.time()
        
        if not self._is_collection_available():
            return "", 0
            
        print(f"[INFO] Starting {self.service_name} export...")
        
        # Step 1: Load all data once (reusing optimized approach)
        self._load_all_data_optimized()
        
        # Step 2: Filter cards by tag based on service criteria
        filtered_cards_by_tag = self._filter_cards_by_tag_fast()
        
        # Step 3: Build hierarchy and calculate counts
        self.tag_counter.build_hierarchy_from_cards(filtered_cards_by_tag)
        hierarchical_data = self.tag_counter.calculate_hierarchical_counts()
        
        # Step 4: Create export data with hierarchical counts
        export_data = self._create_export_data_fast(filtered_cards_by_tag, hierarchical_data)
        
        # Step 5: Compress and save (reusing optimized compression)
        file_path = self._compress_and_save_fast(export_data)
        
        elapsed = time.time() - self.start_time
        print(f"[INFO] {self.service_name} export completed in {elapsed:.2f} seconds")
        
        return file_path, len(export_data)
    
    def _is_collection_available(self) -> bool:
        """Check if Anki collection is available"""
        from aqt import mw
        if mw is None or mw.col is None:
            print(f"[ERROR] Collection not available for {self.service_name}")
            return False
        return True
    
    def _load_all_data_optimized(self):
        """Load all data using the original optimized approach"""
        from aqt import mw
        from collections import defaultdict
        
        print(f"[INFO] Bulk loading all data for {self.service_name}...")
        
        # Reuse the exact same bulk loading approach from optimized_tag_exporter.py
        card_ids = mw.col.find_cards("")  # Empty query returns all cards
        print(f"[INFO] Loading info for {len(card_ids)} cards...")
        
        # Batch card loading for better performance (same as original)
        batch_size = 1000
        for i in range(0, len(card_ids), batch_size):
            batch_ids = card_ids[i:i+batch_size]
            
            # Load cards in bulk using direct SQL for maximum speed (same query)
            cards_data = mw.col.db.all(
                f"""
                SELECT 
                    c.id, c.nid, c.did, c.queue, c.due, c.ivl, c.factor, 
                    c.reps, c.lapses, c.left
                FROM cards c
                WHERE c.id IN ({','.join(str(cid) for cid in batch_ids)})
                """
            )
            
            # Get all note IDs from this batch to fetch tags
            note_ids = set(row[1] for row in cards_data)
            
            # Bulk load notes for this batch (same approach)
            if note_ids:
                notes_data = mw.col.db.all(
                    f"""
                    SELECT n.id, n.tags FROM notes n
                    WHERE n.id IN ({','.join(str(nid) for nid in note_ids)})
                    """
                )
                
                # Create note dictionary with tags
                for nid, tags_str in notes_data:
                    if tags_str:
                        tags = tags_str.strip().split()
                    else:
                        tags = []
                    self.notes_by_id[nid] = {"id": nid, "tags": tags}
            
            # Process card data (same logic)
            for row in cards_data:
                cid, nid, did, queue, due, ivl, factor, reps, lapses, left = row
                
                # Get deck name
                deck_name = mw.col.decks.name(did)
                
                # Create card info dictionary
                card_info = {
                    "id": cid,
                    "nid": nid,
                    "deck": deck_name,
                    "queue": queue,
                    "due": due,
                    "ivl": ivl,
                    "factor": factor,
                    "reps": reps,
                    "lapses": lapses,
                    "left": left,
                    "suspended": queue == -1,
                    "tags": self.notes_by_id.get(nid, {}).get("tags", []),
                    "last_review_date": None  # Will be populated after revlog loads
                }
                
                self.cards_by_id[cid] = card_info
        
        # Load revlog data (same optimized approach)
        self._load_revlog_data_optimized()
    
    def _load_revlog_data_optimized(self):
        """Load review log data using optimized approach"""
        from aqt import mw
        from collections import defaultdict
        from datetime import datetime
        
        print("[INFO] Loading review log data...")
        revlog_by_card = defaultdict(list)
        
        # Use direct SQL for maximum speed (same query)
        revlog_data = mw.col.db.all(
            """
            SELECT id, cid, usn, ease, ivl, lastIvl, factor, time, type 
            FROM revlog
            """
        )
        
        # Track latest review timestamp per card for date extraction
        latest_review_by_card = {}
        
        # Process revlog data (same logic)
        for row in revlog_data:
            log_id, cid, usn, ease, ivl, last_ivl, factor, time_ms, log_type = row
            
            # Only process if we care about this card
            if cid in self.cards_by_id:
                revlog_entry = {
                    "id": log_id,
                    "cid": cid,
                    "usn": usn,
                    "ease": ease,
                    "ivl": ivl,
                    "lastIvl": last_ivl,
                    "factor": factor,
                    "time": time_ms,
                    "type": log_type
                }
                revlog_by_card[cid].append(revlog_entry)
                
                # Track latest review timestamp (revlog 'id' IS the timestamp in milliseconds)
                if cid not in latest_review_by_card or log_id > latest_review_by_card[cid]:
                    latest_review_by_card[cid] = log_id
        
        self.revlog_by_card = revlog_by_card
        
        # Populate last_review_date for each card
        for cid, timestamp_ms in latest_review_by_card.items():
            if cid in self.cards_by_id:
                try:
                    # Convert timestamp (milliseconds) to date string (YYYY-MM-DD)
                    dt = datetime.fromtimestamp(timestamp_ms / 1000)
                    self.cards_by_id[cid]["last_review_date"] = dt.strftime('%Y-%m-%d')
                except Exception as e:
                    # If conversion fails, leave as None
                    print(f"[WARN] Could not convert timestamp {timestamp_ms} for card {cid}: {e}")
                    pass
    
    def _filter_cards_by_tag_fast(self) -> Dict[str, List[Dict]]:
        """Fast filtering using service criteria"""
        criteria = self.get_filter_criteria()
        cards_by_tag = defaultdict(list)
        
        # Fast filtering - process each card once
        for card_info in self.cards_by_id.values():
            for tag in card_info.get("tags", []):
                if self._matches_criteria_fast(tag, criteria):
                    cards_by_tag[tag].append(card_info)
        
        return cards_by_tag
    
    def _matches_criteria_fast(self, tag: str, criteria: Dict[str, Any]) -> bool:
        """Fast criteria matching using optimized string operations"""
        include_patterns = criteria.get("include_patterns", [])
        exclude_patterns = criteria.get("exclude_patterns", [])
        
        # Check exclude patterns first - use any() for early termination
        if exclude_patterns:
            if any(pattern in tag for pattern in exclude_patterns):
                return False
        
        # Check include patterns - empty list means include ALL tags
        if include_patterns:
            if not any(pattern in tag for pattern in include_patterns):
                return False
        # If include_patterns is empty, we include all tags (except excluded ones)
        
        return True
    
    def _is_high_yield_tag(self, tag: str) -> bool:
        """Check if a tag is high yield (case-insensitive)"""
        tag_lower = tag.lower()
        # Check for various high yield patterns
        return ('high' in tag_lower and 'yield' in tag_lower) or 'highyield' in tag_lower or 'lowyield' in tag_lower or 'loweryield' in tag_lower
    
    def _get_yield_level(self, tag: str) -> int:
        """
        Extract yield level (1-5) from a tag.
        Returns 0 if not a yield tag.
        
        Examples:
        - "1-HighYield" -> 1
        - "2-RelativelyHighYield" -> 2
        - "3-HighYield-temporary" -> 3
        - "4-LowerYield" -> 4
        - "5-LowYield" -> 5
        """
        import re
        tag_lower = tag.lower()
        
        # Check if this is a yield tag
        if not self._is_high_yield_tag(tag):
            return 0
        
        # Extract yield level from patterns like "1-HighYield", "2-RelativelyHighYield", etc.
        # Look for tags that start with a number (1-5) followed by dash or underscore
        match = re.match(r'^([1-5])[-_]', tag)
        if match:
            return int(match.group(1))
        
        # Also check for tags within the hierarchy like "::1-HighYield"
        parts = tag.split('::')
        for part in parts:
            match = re.match(r'^([1-5])[-_]', part)
            if match:
                return int(match.group(1))
        
        return 0
    
    def _get_high_yield_cards(self, cards: List[Dict]) -> List[Dict]:
        """Filter cards that have high yield tags with explicit levels (1-5)"""
        high_yield_cards = []
        for card in cards:
            card_tags = card.get("tags", [])
            # Only count cards with explicit yield levels (1-5)
            if any(self._get_yield_level(tag) > 0 for tag in card_tags):
                high_yield_cards.append(card)
        return high_yield_cards
    
    def _get_cards_by_yield_level(self, cards: List[Dict], yield_level: int) -> List[Dict]:
        """Filter cards that have a specific yield level (1-5)"""
        yield_cards = []
        for card in cards:
            card_tags = card.get("tags", [])
            for tag in card_tags:
                if self._get_yield_level(tag) == yield_level:
                    yield_cards.append(card)
                    break  # Don't add same card twice
        return yield_cards
    
    def _create_export_data_fast(self, cards_by_tag: Dict[str, List], hierarchical_data: Dict) -> List[Dict[str, Any]]:
        """Create export data with hierarchical counts - optimized for speed"""
        result = []
        
        # Get all tags from hierarchical data (includes parent tags without direct cards)
        all_tags = set(cards_by_tag.keys()) | set(hierarchical_data.keys())
        
        for tag in all_tags:
            cards = cards_by_tag.get(tag, [])
            hierarchical_info = hierarchical_data.get(tag, {})
            
            # Fast statistics calculation (same as original)
            total_cards = len(cards)
            
            # Calculate high yield card counts (legacy - sum of all levels)
            high_yield_total = 0
            high_yield_new = 0
            high_yield_review = 0
            
            # Calculate separate counts for each yield level (1-5)
            yield_stats = {}
            for level in range(1, 6):
                yield_stats[level] = {
                    'total': 0,
                    'new': 0,
                    'review': 0
                }
            
            # Check if this tag itself is a yield tag
            tag_yield_level = self._get_yield_level(tag)
            
            if tag_yield_level > 0:
                # If the tag itself is a yield tag, all its cards belong to that level
                yield_stats[tag_yield_level]['total'] = total_cards
                yield_stats[tag_yield_level]['new'] = sum(1 for c in cards if c.get("reps", 0) == 0)
                yield_stats[tag_yield_level]['review'] = sum(1 for c in cards if c.get("queue", 0) == 2)
                
                # Legacy high_yield totals (sum of all levels)
                high_yield_total = total_cards
                high_yield_new = yield_stats[tag_yield_level]['new']
                high_yield_review = yield_stats[tag_yield_level]['review']
            else:
                # For non-yield tags, count cards that have yield tags
                # For parent tags (no direct cards), check ALL cards in hierarchy
                if hierarchical_info and hierarchical_info.get('hierarchical_count', 0) > 0 and total_cards == 0:
                    # Parent tag - count yield levels from ALL cards in hierarchy
                    all_cards_in_hierarchy = hierarchical_info.get('all_unique_cards', set())
                    for card_id in all_cards_in_hierarchy:
                        card = self.cards_by_id.get(card_id)
                        if card:
                            card_tags = card.get("tags", [])
                            # Find the yield level for this card
                            card_yield_level = 0
                            for t in card_tags:
                                level = self._get_yield_level(t)
                                if level > 0:
                                    card_yield_level = level
                                    break
                            
                            if card_yield_level > 0:
                                yield_stats[card_yield_level]['total'] += 1
                                if card.get("reps", 0) == 0:
                                    yield_stats[card_yield_level]['new'] += 1
                                if card.get("queue", 0) == 2:
                                    yield_stats[card_yield_level]['review'] += 1
                else:
                    # Leaf tag - count yield levels from direct cards only
                    for level in range(1, 6):
                        level_cards = self._get_cards_by_yield_level(cards, level)
                        yield_stats[level]['total'] = len(level_cards)
                        yield_stats[level]['new'] = sum(1 for c in level_cards if c.get("reps", 0) == 0)
                        yield_stats[level]['review'] = sum(1 for c in level_cards if c.get("queue", 0) == 2)
                
                # Calculate legacy high_yield totals (sum of all levels)
                high_yield_total = sum(yield_stats[level]['total'] for level in range(1, 6))
                high_yield_new = sum(yield_stats[level]['new'] for level in range(1, 6))
                high_yield_review = sum(yield_stats[level]['review'] for level in range(1, 6))
            
            # Calculate card metrics from direct cards OR hierarchical cards for parent tags
            if total_cards == 0:
                # Parent tag with no direct cards - calculate from hierarchical cards
                if hierarchical_info and hierarchical_info.get('hierarchical_count', 0) > 0:
                    all_cards_in_hierarchy = hierarchical_info.get('all_unique_cards', set())
                    due_cards = new_cards = learning_cards = review_cards = mature_cards = suspended_cards = 0
                    for card_id in all_cards_in_hierarchy:
                        card = self.cards_by_id.get(card_id)
                        if not card:
                            continue
                        if card.get("due", 0) >= 0 and not card.get("suspended", False):
                            due_cards += 1
                        if card.get("reps", 0) == 0:
                            new_cards += 1
                        if card.get("queue", 0) == 1:
                            learning_cards += 1
                        if card.get("queue", 0) == 2:
                            review_cards += 1
                        if card.get("ivl", 0) > 21:
                            mature_cards += 1
                        if card.get("suspended", False):
                            suspended_cards += 1
                else:
                    # No direct cards and no hierarchy - empty tag
                    due_cards = new_cards = learning_cards = review_cards = mature_cards = suspended_cards = 0
            else:
                # Leaf tag with direct cards - calculate from direct cards
                due_cards = sum(1 for c in cards if c.get("due", 0) >= 0 and not c.get("suspended", False))
                new_cards = sum(1 for c in cards if c.get("reps", 0) == 0)
                learning_cards = sum(1 for c in cards if c.get("queue", 0) == 1)
                review_cards = sum(1 for c in cards if c.get("queue", 0) == 2)
                mature_cards = sum(1 for c in cards if c.get("ivl", 0) > 21)
                suspended_cards = sum(1 for c in cards if c.get("suspended", False))
            
            # Fast averages calculation from direct cards OR hierarchical cards
            if total_cards > 0:
                # Leaf tag - calculate from direct cards
                total_factor = sum(c.get("factor", 0) for c in cards)
                total_interval = sum(c.get("ivl", 0) for c in cards)
                total_lapses = sum(c.get("lapses", 0) for c in cards)
                
                avg_ease = total_factor / total_cards
                avg_interval = total_interval / total_cards
                
                # Fast unique note count
                note_ids = set(c.get("nid") for c in cards if c.get("nid"))
                unique_notes = len(note_ids)
            elif hierarchical_info and hierarchical_info.get('hierarchical_count', 0) > 0:
                # Parent tag - calculate from hierarchical cards
                all_cards_in_hierarchy = hierarchical_info.get('all_unique_cards', set())
                total_factor = total_interval = total_lapses = 0
                note_ids = set()
                count_cards = 0
                for card_id in all_cards_in_hierarchy:
                    card = self.cards_by_id.get(card_id)
                    if not card:
                        continue
                    count_cards += 1
                    total_factor += card.get("factor", 0)
                    total_interval += card.get("ivl", 0)
                    total_lapses += card.get("lapses", 0)
                    nid = card.get("nid")
                    if nid:
                        note_ids.add(nid)
                avg_ease = (total_factor / count_cards) if count_cards else 0
                avg_interval = (total_interval / count_cards) if count_cards else 0
                unique_notes = len(note_ids)
            else:
                # Empty tag
                total_lapses = avg_ease = avg_interval = unique_notes = 0
            
            # Fast review data calculation from direct cards OR hierarchical cards
            if total_cards > 0:
                # Leaf tag - calculate from direct cards
                card_ids = [c.get("id") for c in cards]
                all_revlogs = []
                for cid in card_ids:
                    all_revlogs.extend(self.revlog_by_card.get(cid, []))
                
                total_reviews = len(all_revlogs)
                total_time_ms = sum(log.get("time", 0) for log in all_revlogs)
                avg_time_ms = total_time_ms / total_reviews if total_reviews else 0
                
                # Count by answer button
                again_count = sum(1 for log in all_revlogs if log.get("ease") == 1)
                hard_count = sum(1 for log in all_revlogs if log.get("ease") == 2)
                good_count = sum(1 for log in all_revlogs if log.get("ease") == 3)
                easy_count = sum(1 for log in all_revlogs if log.get("ease") == 4)
            elif hierarchical_info and hierarchical_info.get('hierarchical_count', 0) > 0:
                # Parent tag - calculate from hierarchical cards
                card_ids = list(hierarchical_info.get('all_unique_cards', set()))
                all_revlogs = []
                for cid in card_ids:
                    all_revlogs.extend(self.revlog_by_card.get(cid, []))
                
                total_reviews = len(all_revlogs)
                total_time_ms = sum(log.get("time", 0) for log in all_revlogs)
                avg_time_ms = (total_time_ms / total_reviews) if total_reviews else 0
                
                # Count by answer button
                again_count = sum(1 for log in all_revlogs if log.get("ease") == 1)
                hard_count = sum(1 for log in all_revlogs if log.get("ease") == 2)
                good_count = sum(1 for log in all_revlogs if log.get("ease") == 3)
                easy_count = sum(1 for log in all_revlogs if log.get("ease") == 4)
            else:
                # Empty tag
                total_reviews = total_time_ms = avg_time_ms = 0
                again_count = hard_count = good_count = easy_count = 0
                
            # OPTIMIZED: Use hierarchical data for unstudied cards calculation
            # For parent tags, calculate unstudied from hierarchical data
            if hierarchical_info and hierarchical_info.get('hierarchical_count', 0) > 0:
                # This is a parent tag - calculate unstudied from hierarchical data
                all_cards = hierarchical_info.get('all_unique_cards', set())
                # Count unstudied cards using fast lookup from self.cards_by_id
                unstudied_cards = sum(1 for card_id in all_cards 
                                    if self.cards_by_id.get(card_id, {}).get("reps", 0) == 0)
            else:
                # This is a leaf tag - use direct cards
                unstudied_cards = sum(1 for c in cards if c.get("reps", 0) == 0)
            
            # Create card details for unstudied analysis (same as original)
            card_details = []
            if total_cards > 0:
                for c in cards:
                    if c.get("reps", 0) == 0:  # Only include unstudied cards
                        card_details.append({
                            "card_id": c.get("id"),
                            "note_id": c.get("nid"), 
                            "deck": c.get("deck"),
                            "queue": c.get("queue"),
                            "due": c.get("due"),
                            "suspended": c.get("suspended", False)
                        })
            
            # Create tag data object with hierarchical information
            tag_data = {
                "tag_name": tag,
                "service_name": self.service_name,
                
                # Original counts (compatible with existing analysis)
                "total_cards": total_cards,
                "unique_notes": unique_notes,
                "due_cards": due_cards,
                "new_cards": new_cards,
                "learning_cards": learning_cards,
                "review_cards": review_cards,
                "mature_cards": mature_cards,
                "suspended_cards": suspended_cards,
                "unstudied_cards": unstudied_cards,
                
                # NEW: High Yield counts for frontend filtering (legacy - sum of all levels)
                "high_yield_total_cards": high_yield_total,
                "high_yield_new_cards": high_yield_new,
                "high_yield_review_cards": high_yield_review,
                
                # NEW: Separate counts for each yield level (1-5)
                "yield_1_total_cards": yield_stats[1]['total'],
                "yield_1_new_cards": yield_stats[1]['new'],
                "yield_1_review_cards": yield_stats[1]['review'],
                "yield_2_total_cards": yield_stats[2]['total'],
                "yield_2_new_cards": yield_stats[2]['new'],
                "yield_2_review_cards": yield_stats[2]['review'],
                "yield_3_total_cards": yield_stats[3]['total'],
                "yield_3_new_cards": yield_stats[3]['new'],
                "yield_3_review_cards": yield_stats[3]['review'],
                "yield_4_total_cards": yield_stats[4]['total'],
                "yield_4_new_cards": yield_stats[4]['new'],
                "yield_4_review_cards": yield_stats[4]['review'],
                "yield_5_total_cards": yield_stats[5]['total'],
                "yield_5_new_cards": yield_stats[5]['new'],
                "yield_5_review_cards": yield_stats[5]['review'],
                
                # NEW: Hierarchical counts
                "hierarchical_total_cards": hierarchical_info.get("hierarchical_count", total_cards),
                "children_cards": hierarchical_info.get("children_count", 0),
                "children_tags": hierarchical_info.get("children_tags", []),
                "parent_tags": hierarchical_info.get("parent_tags", []),
                
                # Original statistics
                "avg_ease": avg_ease,
                "avg_interval": avg_interval,
                "total_lapses": total_lapses,
                
                # Original review data
                "review_data": {
                    "total_reviews": total_reviews,
                    "total_time_ms": total_time_ms,
                    "avg_time_ms": avg_time_ms,
                    "again_count": again_count,
                    "hard_count": hard_count,
                    "good_count": good_count,
                    "easy_count": easy_count
                },
                
                # Original card details
                "unstudied_card_details": card_details,
                
                # Metadata
                "timestamp": datetime.utcnow().isoformat(),
                "export_type": "hierarchical_microservice"
            }
            
            result.append(tag_data)
        
        return result
    
    def _compress_and_save_fast(self, data: List[Dict[str, Any]]) -> str:
        """Compress and save using the original fast compression method"""
        filename = self._generate_export_filename()
        return self._compress_to_gzip_fast(data, filename)
    
    def _generate_export_filename(self) -> str:
        """Generate filename for this service"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            from ..session_store import get_user_auth_info
            username = get_user_auth_info()[0] or "user"
            username = username.split('@')[0]
        except Exception:
            username = "user"
        
        service_slug = self.service_name.lower().replace(" ", "_").replace("&", "and")
        return f"{service_slug}_{username}_{timestamp}.ndjson.gz"
    
    def _compress_to_gzip_fast(self, data: List[Dict[str, Any]], filename: str) -> str:
        """Fast compression using the original optimized method"""
        exports_dir = self._get_exports_dir()
        file_path = os.path.join(exports_dir, filename)
        
        try:
            # Use compression level 1 for best speed (same as original)
            with gzip.open(file_path, 'wb', compresslevel=1) as f:
                for item in data:
                    # Write each object as a separate JSON line (same format)
                    line = json.dumps(item) + '\n'
                    f.write(line.encode('utf-8'))
            
            # Verify file exists after writing (same check)
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                print(f"[INFO] {self.service_name} file saved: {file_path} (Size: {file_size} bytes)")
            else:
                print(f"[ERROR] File was not created: {file_path}")
            
            return file_path
        except Exception as e:
            print(f"[ERROR] Failed to compress {self.service_name} data: {e}")
            return ""
    
    def _get_exports_dir(self) -> str:
        """Get the exports directory path, creating it if needed (same as original)"""
        addon_dir = os.path.dirname(os.path.dirname(__file__))  # Go up one level from services/
        exports_dir = os.path.join(addon_dir, "exports")
        os.makedirs(exports_dir, exist_ok=True)
        return exports_dir
