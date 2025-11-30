import pandas as pd
import sqlite3
import os

# ==========================================
# 1. 설정
# ==========================================
CSV_FILE_PATH = "data/tables/demographic_race.csv" 
DB_FILE_PATH = "eval/my_database.db"

# ==========================================
# 2. 수정된 테이블 스키마 (PRIMARY KEY 제거)
# ==========================================
create_table_sql = """
CREATE TABLE IF NOT EXISTS demographics (
    year INTEGER,
    id TEXT, -- PRIMARY KEY 제거 (중복 허용)
    zipcode TEXT,
    race_total_population INTEGER DEFAULT 0,
    one_race INTEGER DEFAULT 0,
    two_or_more_races INTEGER DEFAULT 0,
    white INTEGER DEFAULT 0,
    black INTEGER DEFAULT 0,
    american_indian_and_alaska_native INTEGER DEFAULT 0,
    asian INTEGER DEFAULT 0,
    native_hawaiian_and_other_pacific_islander INTEGER DEFAULT 0,
    some_other_race INTEGER DEFAULT 0,
    hispanic_or_latino_total INTEGER DEFAULT 0,
    hispanic_or_latino INTEGER DEFAULT 0,
    not_hispanic_or_latino INTEGER DEFAULT 0
);
"""

def build_database():
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ 오류: '{CSV_FILE_PATH}' 파일을 찾을 수 없습니다.")
        return

    print(f"📂 '{CSV_FILE_PATH}' 파일을 읽는 중...")
    
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()

    # 기존 테이블 삭제 후 재생성
    cursor.execute("DROP TABLE IF EXISTS demographics")
    cursor.execute(create_table_sql)
    print("✅ 테이블 스키마 생성 완료 (PRIMARY KEY 제약 제거됨)")

    try:
        df = pd.read_csv(CSV_FILE_PATH)
        df.columns = [c.strip() for c in df.columns]
        
        # 1. Zipcode 정제
        if 'zipcode' in df.columns:
            print("   -> Zipcode 데이터 정제 중...")
            df['zipcode'] = df['zipcode'].astype(str).str.replace('ZCTA5', '', regex=False).str.strip()
        
        # 2. NULL 처리
        print("   -> 빈 값(NULL)을 0으로 채우는 중...")
        df = df.fillna(0)

        # 3. 정수 변환
        print("   -> 숫자 컬럼을 정수형(INT)으로 변환 중...")
        for col in df.columns:
            if col not in ['id', 'zipcode']: 
                try:
                    df[col] = df[col].astype(int)
                except:
                    pass

        # 4. 데이터 삽입
        df.to_sql("demographics", conn, if_exists="append", index=False)
        
        conn.commit()
        print(f"\n🎉 성공! 총 {len(df)}개 행이 저장되었습니다.")

    except Exception as e:
        print(f"❌ 데이터 처리 중 오류 발생: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    build_database()