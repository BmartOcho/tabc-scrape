"""
Enhanced Restaurant Concept Classification using Web Scraping and AI
"""

import logging
import re
import time
import asyncio
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
import requests
from bs4 import BeautifulSoup
import json
import os
from urllib.parse import quote

# AI and NLP imports
AI_AVAILABLE = False
try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.ensemble import RandomForestClassifier
    import numpy as np
    import joblib
    AI_AVAILABLE = True
except ImportError:
    # AI libraries not available - will use rule-based classification only
    pass

logger = logging.getLogger(__name__)

@dataclass
class ConceptClassification:
    """Enhanced result of restaurant concept classification"""
    restaurant_name: str
    address: str
    primary_concept: str
    secondary_concepts: List[str]
    confidence: float
    source: str
    keywords_found: List[str]
    ai_confidence: float = 0.0
    web_data_sources: List[str] = field(default_factory=list)
    price_range: Optional[str] = None
    ambiance_indicators: List[str] = field(default_factory=list)

@dataclass
class WebSourceData:
    """Data collected from a web source"""
    source_name: str
    url: str
    categories: List[str]
    description: str
    price_range: Optional[str]
    rating: Optional[float]
    review_count: Optional[int]
    success: bool
    error_message: Optional[str] = None

class EnhancedRestaurantConceptClassifier:
    """Advanced classifier for restaurant concepts using web scraping and AI"""

    def __init__(self):
        # Enhanced concept categories with more granularity
        self.concept_keywords = {
            'fast_food': {
                'keywords': [
                    'fast food', 'quick service', 'drive thru', 'burger', 'pizza',
                    'fried chicken', 'taco bell', 'mcdonalds', 'wendys', 'burger king',
                    'fast casual', 'quick bite', 'grab and go'
                ],
                'price_range': '$',
                'service_style': 'counter_service'
            },
            'fine_dining': {
                'keywords': [
                    'fine dining', 'upscale', 'white tablecloth', 'sommelier',
                    'degustation', 'tasting menu', 'haute cuisine', 'michelin',
                    'award winning', 'chef driven', 'gourmet'
                ],
                'price_range': '$$$$',
                'service_style': 'formal_service'
            },
            'casual_dining': {
                'keywords': [
                    'casual dining', 'family restaurant', 'american food',
                    'comfort food', 'pub', 'grill', 'bistro', 'neighborhood spot',
                    'local favorite', 'traditional'
                ],
                'price_range': '$$',
                'service_style': 'table_service'
            },
            'ethnic': {
                'keywords': [
                    'mexican', 'italian', 'chinese', 'japanese', 'indian',
                    'thai', 'mediterranean', 'greek', 'french', 'vietnamese',
                    'korean', 'middle eastern', 'latin american', 'fusion'
                ],
                'price_range': '$$',
                'service_style': 'table_service'
            },
            'seafood': {
                'keywords': [
                    'seafood', 'fish', 'lobster', 'crab', 'oyster',
                    'sushi', 'raw bar', 'shellfish', 'fresh catch',
                    'seafood restaurant', 'fish house', 'oyster bar'
                ],
                'price_range': '$$$',
                'service_style': 'table_service'
            },
            'steakhouse': {
                'keywords': [
                    'steakhouse', 'steak house', 'prime rib', 'chophouse',
                    'butcher', 'aged beef', 'steak and seafood', 'cattle',
                    'meat house', 'steak specialist'
                ],
                'price_range': '$$$$',
                'service_style': 'formal_service'
            },
            'cafe': {
                'keywords': [
                    'cafe', 'coffee shop', 'bakery', 'sandwich shop',
                    'deli', 'breakfast', 'brunch', 'coffee house',
                    'pastry shop', 'breakfast spot'
                ],
                'price_range': '$',
                'service_style': 'counter_service'
            },
            'bar': {
                'keywords': [
                    'bar', 'pub', 'tavern', 'lounge', 'sports bar',
                    'nightclub', 'cocktail bar', 'brewery', 'taproom',
                    'brewpub', 'gastropub', 'dive bar'
                ],
                'price_range': '$$',
                'service_style': 'bar_service'
            },
            'fast_casual': {
                'keywords': [
                    'chipotle', 'panera', 'sweetgreen', 'cava', 'dig',
                    'fresh casual', 'healthy fast', 'build your own',
                    'customizable', 'assembly line'
                ],
                'price_range': '$$',
                'service_style': 'counter_service'
            },
            'food_truck': {
                'keywords': [
                    'food truck', 'mobile kitchen', 'street food',
                    'food cart', 'popup', 'mobile eatery'
                ],
                'price_range': '$',
                'service_style': 'counter_service'
            }
        }

        # Compile regex patterns for better matching
        self.compiled_patterns = {}
        for concept, data in self.concept_keywords.items():
            pattern = r'\b(?:' + '|'.join(re.escape(keyword) for keyword in data['keywords']) + r')\b'
            self.compiled_patterns[concept] = re.compile(pattern, re.IGNORECASE)

        # Initialize AI components if available
        self.ai_model = None
        self.vectorizer = None
        if AI_AVAILABLE:
            self._initialize_ai_components()

        # Session for web scraping
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

    def _initialize_ai_components(self):
        """Initialize AI/NLP components for enhanced classification"""
        try:
            # Download required NLTK data
            try:
                nltk.data.find('tokenizers/punkt')
            except LookupError:
                nltk.download('punkt', quiet=True)

            try:
                nltk.data.find('corpora/stopwords')
            except LookupError:
                nltk.download('stopwords', quiet=True)

            try:
                nltk.data.find('corpora/wordnet')
            except LookupError:
                nltk.download('wordnet', quiet=True)

            # Initialize NLP tools
            self.lemmatizer = WordNetLemmatizer()
            self.stop_words = set(stopwords.words('english'))

            # Load or train ML model
            self._load_or_train_model()

        except Exception as e:
            logger.error(f"Failed to initialize AI components: {e}")
            AI_AVAILABLE = False

    def _load_or_train_model(self):
        """Load pre-trained model or train a new one"""
        model_path = "models/concept_classifier_model.pkl"

        if os.path.exists(model_path):
            try:
                self.ai_model = joblib.load(model_path)
                logger.info("Loaded pre-trained concept classification model")
            except Exception as e:
                logger.error(f"Failed to load model: {e}")
                self.ai_model = None
        else:
            logger.info("No pre-trained model found. Will use rule-based classification.")

    def _extract_square_footage_from_text(self, text: str) -> Optional[int]:
        """Extract square footage numbers from text"""
        sqft_patterns = [
            r'(\d{1,3}(?:,\d{3})*)\s*(?:sq\.?\s*ft\.?|square\s+feet?|sqft)',
            r'(\d{1,3}(?:,\d{3})*)\s*(?:sf|square\s+foot)',
            r'building\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'property\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'restaurant\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
        ]

        for pattern in sqft_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                numbers = []
                for match in matches:
                    if isinstance(match, tuple):
                        number_str = match[0]
                    else:
                        number_str = match

                    try:
                        number = int(number_str.replace(',', ''))
                        if 100 <= number <= 100000:
                            numbers.append(number)
                    except ValueError:
                        continue

                if numbers:
                    return max(numbers)
        return None

    def _search_google(self, query: str, num_results: int = 5) -> List[str]:
        """Search Google and return top result URLs"""
        try:
            search_url = f"https://www.google.com/search?q={quote(query)}&num={num_results}"
            response = self.session.get(search_url, timeout=10)

            if response.status_code != 200:
                logger.warning(f"Google search failed with status {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')

            urls = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/url?q='):
                    url = href.split('/url?q=')[1].split('&')[0]
                    if url.startswith('http') and 'google.com' not in url:
                        urls.append(url)
                        if len(urls) >= num_results:
                            break

            return urls

        except Exception as e:
            logger.error(f"Error searching Google: {e}")
            return []

    def _scrape_yelp_business(self, restaurant_name: str, address: str) -> Optional[WebSourceData]:
        """Scrape Yelp business information"""
        try:
            # Search for business on Yelp
            query = f"{restaurant_name} {address}"
            search_url = f"https://www.yelp.com/search?find_desc={quote(query)}&find_loc={quote(address)}"

            response = self.session.get(search_url, timeout=10)
            if response.status_code != 200:
                return WebSourceData("yelp", search_url, [], "", None, None, None, False, f"HTTP {response.status_code}")

            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for the first business listing
            business_link = soup.find('a', {'data-testid': 'biz-name'})
            if not business_link:
                return WebSourceData("yelp", search_url, [], "", None, None, None, False, "No business found")

            business_url = f"https://www.yelp.com{business_link['href']}"

            # Get business details
            detail_response = self.session.get(business_url, timeout=10)
            if detail_response.status_code != 200:
                return WebSourceData("yelp", business_url, [], "", None, None, None, False, f"HTTP {detail_response.status_code}")

            detail_soup = BeautifulSoup(detail_response.text, 'html.parser')

            # Extract categories
            categories = []
            category_elements = detail_soup.find_all('a', {'href': lambda x: x and '/category/' in x})
            for cat in category_elements[:3]:  # Limit to 3 categories
                categories.append(cat.text.strip())

            # Extract description
            description = ""
            desc_element = detail_soup.find('meta', {'property': 'description'})
            if desc_element:
                description = desc_element.get('content', '')

            # Extract price range
            price_range = None
            price_element = detail_soup.find('span', {'class': lambda x: x and 'priceRange' in x})
            if price_element:
                price_range = price_element.text.strip()

            # Extract rating
            rating = None
            rating_element = detail_soup.find('span', {'class': lambda x: x and 'rating' in x})
            if rating_element:
                try:
                    rating = float(rating_element.text.strip())
                except ValueError:
                    pass

            return WebSourceData(
                source_name="yelp",
                url=business_url,
                categories=categories,
                description=description,
                price_range=price_range,
                rating=rating,
                review_count=None,  # Would need additional parsing
                success=True
            )

        except Exception as e:
            logger.error(f"Error scraping Yelp: {e}")
            return WebSourceData("yelp", "", [], "", None, None, None, False, str(e))

    def _scrape_google_business(self, restaurant_name: str, address: str) -> Optional[WebSourceData]:
        """Scrape Google Business information"""
        try:
            query = f"{restaurant_name} {address}"
            search_url = f"https://www.google.com/search?q={quote(query)}"

            response = self.session.get(search_url, timeout=10)
            if response.status_code != 200:
                return WebSourceData("google", search_url, [], "", None, None, None, False, f"HTTP {response.status_code}")

            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for business listing in search results
            business_card = soup.find('div', {'class': lambda x: x and 'VkpGBb' in x})
            if not business_card:
                return WebSourceData("google", search_url, [], "", None, None, None, False, "No business found")

            # Extract categories
            categories = []
            category_spans = business_card.find_all('span', {'class': lambda x: x and 'YhemCb' in x})
            for span in category_spans[:3]:
                categories.append(span.text.strip())

            # Extract description
            description = ""
            desc_div = business_card.find('div', {'class': lambda x: x and 'Chtupc' in x})
            if desc_div:
                description = desc_div.text.strip()

            # Extract price range (Google uses · separator)
            price_range = None
            if '·' in business_card.text:
                parts = business_card.text.split('·')
                for part in parts:
                    if any(char in part for char in ['$', '£', '€']):
                        price_range = part.strip()
                        break

            return WebSourceData(
                source_name="google",
                url=search_url,
                categories=categories,
                description=description,
                price_range=price_range,
                rating=None,
                review_count=None,
                success=True
            )

        except Exception as e:
            logger.error(f"Error scraping Google Business: {e}")
            return WebSourceData("google", "", [], "", None, None, None, False, str(e))

    def _ai_classify_text(self, text: str) -> Tuple[str, float]:
        """Use AI/ML to classify restaurant concept from text"""
        if not AI_AVAILABLE or not self.ai_model:
            return self._rule_based_classify(text)

        try:
            # Preprocess text
            tokens = word_tokenize(text.lower())
            filtered_tokens = [self.lemmatizer.lemmatize(word) for word in tokens if word not in self.stop_words and word.isalpha()]

            # Create feature vector (simplified version)
            text_features = ' '.join(filtered_tokens)

            # Use ML model for prediction
            prediction = self.ai_model.predict([text_features])
            probabilities = self.ai_model.predict_proba([text_features])

            concept = prediction[0]
            confidence = float(np.max(probabilities))

            return concept, confidence

        except Exception as e:
            logger.error(f"AI classification error: {e}")
            return self._rule_based_classify(text)

    def _rule_based_classify(self, text: str) -> Tuple[str, float]:
        """Fallback rule-based classification"""
        text_lower = text.lower()
        found_concepts = []
        found_keywords = []

        # Check each concept category
        for concept, pattern in self.compiled_patterns.items():
            matches = pattern.findall(text_lower)
            if matches:
                found_concepts.append(concept)
                found_keywords.extend(matches)

        # Determine primary concept
        primary_concept = found_concepts[0] if found_concepts else 'unknown'

        # Calculate confidence
        confidence = min(len(found_keywords) * 0.2, 1.0)

        return primary_concept, confidence

    def classify_from_name_and_description(self, name: str, description: str = "", address: str = "") -> ConceptClassification:
        """Enhanced classification based on name and description"""
        text_to_analyze = f"{name} {description} {address}".lower()

        # Get AI classification if available
        ai_concept, ai_confidence = self._ai_classify_text(text_to_analyze)

        # Get rule-based classification for comparison
        rule_concept, rule_confidence = self._rule_based_classify(text_to_analyze)

        # Combine results
        if ai_confidence > rule_confidence:
            primary_concept = ai_concept
            confidence = ai_confidence
            source = 'ai_classification'
        else:
            primary_concept = rule_concept
            confidence = rule_confidence
            source = 'rule_based'

        # Find secondary concepts
        secondary_concepts = []
        all_keywords = []

        for concept, pattern in self.compiled_patterns.items():
            matches = pattern.findall(text_to_analyze)
            if matches and concept != primary_concept:
                secondary_concepts.append(concept)
                all_keywords.extend(matches)

        return ConceptClassification(
            restaurant_name=name,
            address=address,
            primary_concept=primary_concept,
            secondary_concepts=secondary_concepts[:3],  # Limit to top 3
            confidence=confidence,
            source=source,
            keywords_found=list(set(all_keywords)),
            ai_confidence=ai_confidence
        )

    def scrape_concept_from_web(self, restaurant_name: str, address: str) -> ConceptClassification:
        """Enhanced web scraping for restaurant concept classification"""
        logger.info(f"Scraping concept for {restaurant_name} at {address}")

        web_data_sources = []

        # Try multiple web sources
        sources_to_try = [
            ('yelp', lambda: self._scrape_yelp_business(restaurant_name, address)),
            ('google', lambda: self._scrape_google_business(restaurant_name, address))
        ]

        for source_name, scrape_func in sources_to_try:
            try:
                source_data = scrape_func()
                if source_data and source_data.success:
                    web_data_sources.append(source_data.source_name)

                    # Combine all text data for classification
                    all_text = f"{restaurant_name} {address} {' '.join(source_data.categories)} {source_data.description}"

                    # Get classification based on scraped data
                    classification = self.classify_from_name_and_description(
                        restaurant_name,
                        all_text,
                        address
                    )

                    # Enhance with web data
                    classification.web_data_sources = web_data_sources
                    classification.price_range = source_data.price_range

                    # Boost confidence if we got data from multiple sources
                    if len(web_data_sources) > 1:
                        classification.confidence = min(classification.confidence * 1.3, 1.0)

                    return classification

            except Exception as e:
                logger.error(f"Error scraping {source_name}: {e}")
                continue

        # If web scraping failed, fall back to name-based classification
        fallback_classification = self.classify_from_name_and_description(restaurant_name, "", address)
        fallback_classification.source = 'fallback_name_analysis'
        fallback_classification.web_data_sources = web_data_sources

        return fallback_classification

    def classify_restaurant(self, restaurant_name: str, address: str, description: str = "") -> ConceptClassification:
        """Main method to classify a restaurant's concept"""
        # Try web scraping first for better accuracy
        web_result = self.scrape_concept_from_web(restaurant_name, address)

        # If web scraping gives poor results, enhance with provided description
        if web_result.confidence < 0.4 and description:
            enhanced_text = f"{web_result.restaurant_name} {description} {address}"
            name_result = self.classify_from_name_and_description(restaurant_name, enhanced_text, address)

            # Use the better result
            if name_result.confidence > web_result.confidence:
                name_result.web_data_sources = web_result.web_data_sources
                name_result.price_range = web_result.price_range
                return name_result

        return web_result

    def classify_multiple_restaurants(self, restaurants: List[Dict[str, Any]]) -> Dict[str, ConceptClassification]:
        """Classify concepts for multiple restaurants with rate limiting"""
        results = {}

        for i, restaurant in enumerate(restaurants):
            logger.info(f"Classifying restaurant {i+1}/{len(restaurants)}: {restaurant.get('location_name', 'Unknown')}")

            restaurant_id = restaurant.get('id', restaurant.get('location_name', 'unknown'))
            name = restaurant.get('location_name', '')
            address = restaurant.get('full_address', restaurant.get('location_address', ''))

            try:
                result = self.classify_restaurant(name, address)
                results[restaurant_id] = result

                # Rate limiting: 1 second between requests
                if i < len(restaurants) - 1:
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error classifying {name}: {e}")
                results[restaurant_id] = ConceptClassification(
                    restaurant_name=name,
                    address=address,
                    primary_concept='unknown',
                    secondary_concepts=[],
                    confidence=0.0,
                    source='error',
                    keywords_found=[]
                )

        return results

    def get_classification_stats(self, results: Dict[str, ConceptClassification]) -> Dict[str, Any]:
        """Get statistics about classification results"""
        if not results:
            return {}

        total = len(results)
        successful = sum(1 for r in results.values() if r.primary_concept != 'unknown')
        failed = total - successful

        # Concept distribution
        concepts = {}
        for result in results.values():
            if result.primary_concept != 'unknown':
                concepts[result.primary_concept] = concepts.get(result.primary_concept, 0) + 1

        # Source analysis
        sources = {}
        for result in results.values():
            sources[result.source] = sources.get(result.source, 0) + 1

        # Confidence analysis
        avg_confidence = sum(r.confidence for r in results.values()) / total
        high_confidence = sum(1 for r in results.values() if r.confidence > 0.7)

        return {
            'total_restaurants': total,
            'successful_classifications': successful,
            'failed_classifications': failed,
            'success_rate': successful / total if total > 0 else 0,
            'concept_distribution': concepts,
            'source_distribution': sources,
            'average_confidence': avg_confidence,
            'high_confidence_rate': high_confidence / total if total > 0 else 0,
            'web_data_coverage': sum(1 for r in results.values() if r.web_data_sources) / total if total > 0 else 0
        }