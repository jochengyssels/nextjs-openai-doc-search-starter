import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

export async function runKitespotAgent() {
  try {
    // Fetch rows from the "kitespots" table
    const { data: kitespots, error } = await supabase
      .from('kitespots')
      .select('id, name, description')
      .is('description', null); // Only fetch spots that need enriching

    if (error) throw error;

    return kitespots;
  } catch (error) {
    console.error('Error fetching kitespots:', error);
    return null;
  }
}
