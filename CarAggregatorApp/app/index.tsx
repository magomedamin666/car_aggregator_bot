// app/index.tsx
import { useState, useEffect } from 'react';
import { 
  FlatList, 
  Text, 
  View, 
  ActivityIndicator, 
  TextInput, 
  TouchableOpacity, 
  StyleSheet,
  Linking,
  SafeAreaView
} from 'react-native';
import axios from 'axios';

// 👇 ЗАМЕНИТЕ НА ВАШ РЕАЛЬНЫЙ IP (тот же, что в api_server.py)
const API_BASE = 'http://192.168.1.105:8000';

// Тип данных (parsed_at приходит как ISO-строка из JSON)
interface Ad {
  id: number;
  title: string;
  price: number | null;
  brand: string | null;
  model: string | null;
  year: number | null;
  mileage: number | null;
  region: string | null;
  url: string;
  photo_url: string | null;
  parsed_at: string | null;
}

export default function HomeScreen() {
  const [ads, setAds] = useState<Ad[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchBrand, setSearchBrand] = useState('');

  const fetchAds = async (brand = '') => {
    try {
      setLoading(true);
      setError(null);
      const url = `${API_BASE}/ads?limit=20${brand ? `&brand=${brand}` : ''}`;
      console.log('📡 Запрос к API:', url);
      
      const response = await axios.get(url);
      console.log('✅ Ответ:', response.data.length, 'объявлений');
      setAds(response.data);
    } catch (err: any) {
      console.error('❌ Ошибка загрузки:', err);
      if (err.response) {
        setError(`Сервер ответил: ${err.response.status} ${err.response.data?.detail || ''}`);
      } else if (err.request) {
        setError('Нет ответа от сервера. Проверьте IP и сеть.');
      } else {
        setError(err.message || 'Неизвестная ошибка');
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAds();
  }, []);

  const handleSearch = () => {
    fetchAds(searchBrand);
  };

  const renderItem = ({ item }: { item: Ad }) => (
    <TouchableOpacity 
      style={styles.card}
      onPress={() => Linking.openURL(item.url).catch(() => {})}
      activeOpacity={0.7}
    >
      <View style={styles.info}>
        <Text style={styles.title} numberOfLines={2}>{item.title}</Text>
        <Text style={styles.price}>
          {item.price ? `${item.price.toLocaleString('ru-RU')} ₽` : 'Цена не указана'}
        </Text>
        <Text style={styles.details}>
          {item.brand || 'Марка не указана'} {item.model || ''} • {item.year || '?'} г.
        </Text>
        <Text style={styles.details}>
          🛣 {item.mileage ? `${item.mileage.toLocaleString('ru-RU')} км` : '? км'} • 📍 {item.region || '?'}
        </Text>
        {item.parsed_at && (
          <Text style={styles.date}>
            📅 Добавлено: {new Date(item.parsed_at).toLocaleDateString('ru-RU')}
          </Text>
        )}
      </View>
    </TouchableOpacity>
  );

  // ✅ Исправленный компонент для пустого списка
  const renderEmptyList = () => {
    if (loading) return null;
    return (
      <View style={styles.emptyContainer}>
        <Text style={styles.empty}>Нет объявлений</Text>
        <Text style={styles.emptySub}>Попробуйте изменить фильтры</Text>
      </View>
    );
  };

  if (loading && ads.length === 0) {
    return (
      <View style={styles.center}>
        <ActivityIndicator size="large" color="#007AFF" />
        <Text style={styles.loadingText}>Загрузка объявлений...</Text>
      </View>
    );
  }

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.header}>
        <Text style={styles.headerTitle}>🚗 Авто из berkat.ru</Text>
        
        <View style={styles.searchRow}>
          <TextInput
            style={styles.searchInput}
            placeholder="Марка (например, Lada)"
            value={searchBrand}
            onChangeText={setSearchBrand}
            onSubmitEditing={handleSearch}
            returnKeyType="search"
          />
          <TouchableOpacity style={styles.searchBtn} onPress={handleSearch}>
            <Text style={styles.searchBtnText}>🔍</Text>
          </TouchableOpacity>
        </View>
      </View>

      {error && (
        <View style={styles.errorBox}>
          <Text style={styles.errorText}>❌ {error}</Text>
          <TouchableOpacity onPress={() => fetchAds()} style={styles.retryBtn}>
            <Text style={styles.retryText}>Повторить</Text>
          </TouchableOpacity>
        </View>
      )}

      <FlatList
        data={ads}
        renderItem={renderItem}
        keyExtractor={(item) => item.id.toString()}
        onRefresh={() => fetchAds(searchBrand)}
        refreshing={loading}
        contentContainerStyle={styles.list}
        ListEmptyComponent={renderEmptyList}
      />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#f5f5f5' },
  center: { flex: 1, justifyContent: 'center', alignItems: 'center' },
  loadingText: { marginTop: 12, color: '#666' },
  header: { padding: 16, backgroundColor: '#fff', borderBottomWidth: 1, borderColor: '#eee' },
  headerTitle: { fontSize: 22, fontWeight: 'bold', marginBottom: 12 },
  searchRow: { flexDirection: 'row', gap: 8 },
  searchInput: { flex: 1, padding: 12, borderWidth: 1, borderColor: '#ddd', borderRadius: 8, backgroundColor: '#fafafa' },
  searchBtn: { padding: 12, backgroundColor: '#007AFF', borderRadius: 8, justifyContent: 'center', paddingHorizontal: 16 },
  searchBtnText: { color: '#fff', fontSize: 18 },
  list: { padding: 12 },
  card: { 
    backgroundColor: '#fff', 
    borderRadius: 12, 
    padding: 12, 
    shadowColor: '#000', 
    shadowOffset: { width: 0, height: 1 }, 
    shadowOpacity: 0.1, 
    shadowRadius: 2, 
    elevation: 2 
  },
  info: { gap: 4 },
  title: { fontSize: 16, fontWeight: '600', lineHeight: 20 },
  price: { fontSize: 18, fontWeight: 'bold', color: '#007AFF', marginVertical: 4 },
  details: { fontSize: 14, color: '#666', lineHeight: 18 },
  date: { fontSize: 12, color: '#999', marginTop: 6 },
  errorBox: { margin: 12, padding: 12, backgroundColor: '#ffebee', borderRadius: 8, alignItems: 'center' },
  errorText: { color: '#c62828', marginBottom: 8 },
  retryBtn: { paddingVertical: 6, paddingHorizontal: 16, backgroundColor: '#c62828', borderRadius: 4 },
  retryText: { color: '#fff', fontSize: 14 },
  empty: { textAlign: 'center', marginTop: 40, color: '#999', fontSize: 16 },
  emptyContainer: { alignItems: 'center', marginTop: 40 },
  emptySub: { textAlign: 'center', color: '#bbb', fontSize: 14, marginTop: 4 },
});