# import torch
# import torch.nn.functional as F
from src.JinaAI import JinaAIEmbedder

model = JinaAIEmbedder()


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


def extract_specific_word_embedding(
        model, tokenizer, sentence, start_marker='<w>', end_marker='</w>'):
    """_summary_  # TODO: fill out docstring

    Args:
        model (_type_): _description_
        tokenizer (_type_): _description_
        sentence (_type_): _description_
        start_marker (str, optional): _description_. Defaults to '<w>'.
        end_marker (str, optional): _description_. Defaults to '</w>'.

    Returns:
        _type_: _description_
    """

    # Check if the markers are in the sentence
    if start_marker in sentence and end_marker in sentence:
        # Find the position of the markers
        start_index = sentence.find(start_marker)
        end_index = sentence.find(end_marker) + len(end_marker)

        # Split the sentence into three parts: before, target, and after
        mid_left_index = start_index + len(start_marker)
        mid_right_index = end_index - len(end_marker)
        before_target = sentence[:start_index]
        target_word = sentence[mid_left_index:mid_right_index]
        after_target = sentence[end_index:]

        # Clean the sentence by removing the markers
        tokens_before = tokenizer(
            before_target,
            add_special_tokens=False
        )['input_ids']

        tokens_target = tokenizer(
            target_word,
            add_special_tokens=False
        )['input_ids']

        # Reconstruct the input with markers removed and tokenize
        clean_sentence = before_target + target_word + after_target
        inputs = tokenizer(
            clean_sentence,
            return_tensors="pt"
        ).to(model.device)

        # Run the model to get output embeddings
        task = 'retrieval.query'
        task_id = model._adaptation_map[task]
        adapter_mask = torch.full(
            (1,),
            task_id,
            dtype=torch.int32
        ).to(model.device)

        with torch.no_grad():
            outputs = model(**inputs, adapter_mask=adapter_mask)

        # Calculate the start and end positions of the target word tokens
        target_start_pos = len(tokens_before)
        target_end_pos = target_start_pos + len(tokens_target)

        target_attention_mask = torch.zeros_like(inputs['attention_mask'])
        target_attention_mask[:, target_start_pos:target_end_pos] = 1

        # Apply mean pooling only to the target word's embeddings
        # word_embeddings = mean_pooling(outputs, target_attention_mask)
        # normalized_embeddings = F.normalize(word_embeddings, p=2, dim=1)

        normalized_embeddings = (
            outputs[0][:, target_start_pos:target_end_pos].mean(dim=0)
        )
        return normalized_embeddings
    else:
        with torch.no_grad():
            outputs = model.encode(sentence, task='retrieval.query')
        return outputs


# Example usage
sentence = (
    "I went to the river bank to take a swim. "
    "Afterwards, I went to the <w>bank</w> to withdraw some money."
)
embedding = extract_specific_word_embedding(
    model.model,
    model.tokenizer,
    sentence
)
print("Extracted Embedding:", embedding)
