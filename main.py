import pandas as pd
from nettoyage import preprocess_tickets
from traitement_ai import extract_ticket_info
from stockage import store_ticket_in_mongo, get_collection_stats

def main():
    print("🚀 Démarrage du pipeline de traitement des tickets Jira...")
   
    # Étape 1 : Charger et nettoyer les tickets
    fichier_excel = "C:/Users/gouja/Downloads/les100deuxièmelignes.xlsx"
    
    print(f"📁 Chargement du fichier: {fichier_excel}")
   
    df_propre = preprocess_tickets(fichier_excel)
    print(f"🧹 Nettoyage terminé - {len(df_propre)} tickets traités")
   
    # Étape 2 : Extraire les informations avec l'IA
    print("\n🧠 Extraction des informations avec l'IA...")
    extracted_data = []
   
    for index, row in df_propre.iterrows():
        print(f"  Traitement du ticket {index+1}/{len(df_propre)}...")
        ticket_id = row.get('key', '')  # Utiliser l'ID original du fichier

        ticket_text = " ".join(str(value) for value in row.values if pd.notna(value))
        summary = extract_ticket_info(ticket_text)

        if summary:
            # Initialiser les variables
            problem = "Inconnu"
            solution = "Non résolu"
            keywords = ""

            # Extraire proprement les informations
            lines = summary.split("\n")
            for i, line in enumerate(lines):
                line = line.strip()
               
                if line.lower().startswith("### problématique"):
                    problem = lines[i+1].strip() if i+1 < len(lines) and lines[i+1].strip() else "Inconnu"
                elif line.lower().startswith("### solution"):
                    solution = lines[i+1].strip() if i+1 < len(lines) and lines[i+1].strip() else "Non résolu"
                elif line.lower().startswith("### mots-clés techniques"):
                    keywords = lines[i+1].strip() if i+1 < len(lines) and lines[i+1].strip() else ""

            
            # Ajouter au tableau extrait
            extracted_data.append({
                "ID": ticket_id,
                "Problématique": problem,
                "Solution": solution,
                "Mots-clés techniques": keywords,
                "Texte brut": ticket_text
            })

    # Convertir en DataFrame et sauvegarder dans un fichier CSV
    if extracted_data:
        df_extracted = pd.DataFrame(extracted_data)
        output_file = "résultat_résumé_tickets.csv"
        df_extracted.to_csv(output_file, index=False)
        print(f"📝 Données extraites sauvegardées dans {output_file}")

        # Étape 3 : Stocker dans MongoDB
        print("\n💾 Stockage des tickets dans MongoDB...")
        for index, row in df_extracted.iterrows():
            ticket_data = {
                "ID": row["ID"],
                "problem": row["Problématique"],
                "solution": row["Solution"],
                "keywords": row["Mots-clés techniques"]
            }
            store_ticket_in_mongo(ticket_data)

       
        # Vérifier les statistiques de la collection
        stats = get_collection_stats()
        print(f"📊 Base de données: {stats['count']} tickets stockés au total")
       
        print("\n✅ Pipeline terminé avec succès!")
    else:
        print("❌ Aucune donnée extraite. Vérifiez la connexion à l'API IA.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Erreur dans le pipeline: {e}")