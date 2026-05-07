# TicketCounter

Modernize edilmiş masaüstü destek sayacı uygulaması.

## Ne yapar?
- Excel / XLS / CSV dosyalarından destek kayıtlarını okur
- İleride gerçek endpoint bağlamak için API üzerinden JSON veri çekmeye hazırdır
- Atanan destek personeline göre ticket sayılarını hesaplar
- Tarih aralığına göre filtreleme yapar
- Toplam bütçeyi ticket yüzdesine göre kişi bazlı dağıtır
- Sonuçları tablo + pie chart + özet kartlar halinde gösterir
- Sonuç özetini CSV olarak dışa aktarır

## Veri kaynakları
### 1) Excel / CSV
Aşağıdaki formatlar desteklenir:
- `.xlsx`
- `.xls`
- `.csv`

Kolon isimleri Türkçe export başlıklarıyla eşlenir. Özellikle şu alanlar desteklenir:
- `Atanan Destek Personeli`
- `Oluşturma Tarihi`
- `Güncelleme Tarihi`
- `Kapatan Kullanıcı`

### 2) API
Arayüz artık API veri kaynağına hazırdır.

Beklenen yapı:
- doğrudan liste dönen JSON
- veya şu alanlardan birinde liste: `data`, `results`, `items`, `tickets`, `records`

Örnek:
```json
{
  "data": [
    {
      "Atanan Destek Personeli": "Ahmet",
      "Oluşturma Tarihi": "2026-05-01 10:00:00",
      "Güncelleme Tarihi": "2026-05-01 11:00:00"
    }
  ]
}
```

Bearer token opsiyoneldir.

## Kurulum
Python 3.11+ önerilir.

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python src/app_tk.py
```

## EXE alma
PyInstaller dependency olarak eklendi.

Mevcut spec dosyasıyla örnek build:
```bash
pyinstaller SupportSayaci.spec
```

## Mevcut durum
Bu sürümde yapılanlar:
- eski sade Tkinter görünümü modern desktop UI ile yenilendi
- veri katmanı dosya ve API kaynakları için ayrıldı
- Excel içe alma desteği korundu
- bütçe girilerek kişi bazlı yüzde ve hakediş hesabı eklendi
- ileride gerçek ticket API’sine bağlanmak için temel entegrasyon hazırlandı

## Not
Gerçek API endpoint’i ve auth yapısı netleştiğinde sadece mapping / endpoint uyarlaması yapılarak kullanılabilir.
