from collections import defaultdict
from typing import Dict, List, Set, Tuple, Any
import time

class HierarchicalTagCounter:
    """High-performance hierarchical tag counter with unique card tracking"""
    
    def __init__(self):
        self.tag_hierarchy = defaultdict(set)
        self.card_to_tags = defaultdict(set)  # card_id -> set of tags
        self.tag_to_cards = defaultdict(set)  # tag -> set of card_ids
        self.unique_card_ids = set()
        self._hierarchy_built = False
        
    def build_hierarchy_from_cards(self, cards_by_tag: Dict[str, List[Dict]]) -> None:
        """Build hierarchy and track unique cards from card data - optimized"""
        if self._hierarchy_built:
            return
            
        print("[INFO] Building tag hierarchy and tracking unique cards...")
        start_time = time.time()
        
        # Single pass through all cards to build mappings
        for tag, cards in cards_by_tag.items():
            card_ids = set()
            
            for card in cards:
                card_id = card.get('id')
                if card_id:
                    card_ids.add(card_id)
                    self.unique_card_ids.add(card_id)
                    self.card_to_tags[card_id].add(tag)
            
            self.tag_to_cards[tag] = card_ids
            
            # Build hierarchy from tag structure - optimized
            self._build_tag_hierarchy_fast(tag)
        
        self._hierarchy_built = True
        elapsed = time.time() - start_time
        print(f"[INFO] Hierarchy built in {elapsed:.3f}s - {len(self.unique_card_ids)} unique cards, {len(self.tag_to_cards)} tags")
    
    def _build_tag_hierarchy_fast(self, tag_name: str) -> None:
        """Fast hierarchy building using string operations"""
        if '::' not in tag_name:
            return
            
        # Split once and reuse
        tag_parts = tag_name.split('::')
        
        # Build all parent-child relationships in one pass
        for i in range(len(tag_parts) - 1):
            parent = '::'.join(tag_parts[:i+1])
            child = '::'.join(tag_parts[:i+2])
            self.tag_hierarchy[parent].add(child)
            
    
    def calculate_hierarchical_counts(self) -> Dict[str, Dict[str, Any]]:
        """Calculate hierarchical counts with unique card tracking - optimized"""
        print("[INFO] Calculating hierarchical card counts...")
        start_time = time.time()
        
        # Get ALL tags: both those with direct cards AND parent-only tags from hierarchy
        all_tags = set(self.tag_to_cards.keys()) | set(self.tag_hierarchy.keys())
        
        # Sort tags by depth (deepest first) for bottom-up calculation
        sorted_tags = sorted(
            all_tags, 
            key=lambda x: -x.count('::')
        )
        
        hierarchical_data = {}
        
        # Process tags from deepest to shallowest for efficiency
        for tag in sorted_tags:
            # Get direct cards for this tag
            direct_cards = self.tag_to_cards.get(tag, set())
            direct_count = len(direct_cards)
            
            # Get all unique cards from children (using pre-calculated data)
            all_cards = direct_cards.copy()
            children = self.tag_hierarchy.get(tag, set())
            
            for child in children:
                if child in hierarchical_data:
                    # Use already calculated child data
                    child_cards = hierarchical_data[child]['all_unique_cards']
                else:
                    # Fallback to direct cards
                    child_cards = self.tag_to_cards.get(child, set())
                
                all_cards.update(child_cards)
            
            hierarchical_count = len(all_cards)
            children_count = hierarchical_count - direct_count
            
            hierarchical_data[tag] = {
                'direct_cards': direct_cards,
                'all_unique_cards': all_cards,
                'direct_count': direct_count,
                'hierarchical_count': hierarchical_count,
                'children_count': children_count,
                'children_tags': list(children),
                'parent_tags': self._get_parent_tags_fast(tag)
            }
        
        elapsed = time.time() - start_time
        print(f"[INFO] Hierarchical counts calculated in {elapsed:.3f}s")
        return hierarchical_data
    
    def _get_parent_tags_fast(self, tag_name: str) -> List[str]:
        """Fast parent tag calculation using pre-split parts"""
        if '::' not in tag_name:
            return []
            
        parts = tag_name.split('::')
        return ['::'.join(parts[:i]) for i in range(1, len(parts))]
    
    def get_unique_card_count(self) -> int:
        """Get total number of unique cards across all tags"""
        return len(self.unique_card_ids)
    
    def get_tag_statistics(self) -> Dict[str, Any]:
        """Get comprehensive tag statistics"""
        hierarchical_data = self.calculate_hierarchical_counts()
        
        return {
            'total_unique_cards': len(self.unique_card_ids),
            'total_tags': len(self.tag_to_cards),
            'hierarchical_tags': len(self.tag_hierarchy),
            'max_depth': max((tag.count('::') for tag in self.tag_to_cards.keys()), default=0),
            'hierarchical_data': hierarchical_data
        }
    
    def reset(self):
        """Reset all data for new calculation"""
        self.tag_hierarchy.clear()
        self.card_to_tags.clear()
        self.tag_to_cards.clear()
        self.unique_card_ids.clear()
        self._hierarchy_built = False
